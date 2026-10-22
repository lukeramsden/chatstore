import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.cache import Cache
from chatstore.cli.app import main
from chatstore.config import load_config, load_identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir
from tests.fixtures.synthetic import build_whatsapp

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS guarded native collector')


@pytest.fixture
def collector(tmp_path, monkeypatch):
    root = build_whatsapp(tmp_path / 'whatsapp', extra_messages=50)
    dd = DataDir(tmp_path / 'data')
    monkeypatch.setenv('CHATSTORE_DATA_DIR', str(dd.root))
    with closing(sqlite3.connect(root / 'ChatStorage.sqlite')) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
        writer.commit()
        yield root, dd, writer


def cli(capsys, *args):
    code = main(['--json', *args])
    result = json.loads(capsys.readouterr().out)
    assert code == 0, result
    return result['data']


def test_cli_whatsapp_dispatch_preserves_old_changes_and_optional_creation(collector, capsys):
    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    initial = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    capture = initial['snapshot']['prepared_native']['capture']
    assert capture['baseline']
    assert capture['context']['whatsapp_auxiliary']['files']['ContactsV2'] == {'present': False}
    clean = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert clean['counts'].get('messages', 0) == 0
    writer.execute("UPDATE ZWAMESSAGE SET ZTEXT='old edit through WhatsApp CLI' WHERE Z_PK=1")
    writer.commit()
    edited = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert edited['mode'] == 'incremental' and edited['counts']['messages'] == 1
    with closing(sqlite3.connect(root / 'ContactsV2.sqlite')) as contacts:
        contacts.execute('CREATE TABLE ZWAADDRESSBOOKCONTACT(ZLID TEXT,ZWHATSAPPID TEXT)')
        contacts.execute("INSERT INTO ZWAADDRESSBOOKCONTACT VALUES('123456@lid','15550000000@s.whatsapp.net')")
        contacts.commit()
    auxiliary = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert auxiliary['counts'].get('messages', 0) == 0 and auxiliary['counts']['aliases'] == 3
    assert auxiliary['snapshot']['prepared_native']['capture']['context']['whatsapp_auxiliary']['files']['ContactsV2']['present']
    writer.execute('DELETE FROM ZWAMESSAGE WHERE Z_PK=1')
    writer.commit()
    deleted = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert deleted['counts'].get('messages', 0) == 0
    with closing(Cache(dd.cache)) as cache:
        first = next(row for row in cache.iter_records('messages') if row['text'] == 'old edit through WhatsApp CLI')
        assert cache.observations_of(first['urn'])[0]['present'] == 'absent_from_source'
        assert len(cache.revisions_of(first['urn'])) == 2


def test_cli_whatsapp_pending_retry_does_not_open_expired_sources(collector, capsys, monkeypatch):
    root, dd, _ = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    def interrupted(*args, **kwargs):
        raise RuntimeError('interrupted WhatsApp canonical write')
    with closing(Cache(dd.cache)) as cache, monkeypatch.context() as patch:
        patch.setattr(cache, 'upsert_many', interrupted)
        with pytest.raises(RuntimeError, match='interrupted WhatsApp'):
            collect_native(WhatsAppAdapter(), dd, load_config(dd), load_identity(dd), cache)
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        pending = mirror.pending()
        assert pending is not None
    for name in ('ChatStorage', 'LID'):
        (root / f'{name}.sqlite').rename(root / f'offline-{name}.sqlite')
    retried = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert retried['snapshot']['prepared_native']['capture']['token'] == pending.token
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation() == pending


def test_file_path_transition_rolls_back_native_candidate_before_fallback(collector, capsys):
    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    initial = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    token = initial['snapshot']['prepared_native']['capture']['token']
    writer.execute("UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH='Media/x/pic.jpg' WHERE Z_PK=1")
    writer.commit()
    with closing(Cache(dd.cache)) as cache:
        assert collect_native(WhatsAppAdapter(), dd, load_config(dd), load_identity(dd), cache) is None
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation().token == token
        assert mirror.conn.execute('SELECT ZMEDIALOCALPATH FROM ZWAMEDIAITEM WHERE Z_PK=1').fetchone()[0] is None
    fallback = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert 'prepared_native' not in fallback['snapshot']
    with closing(Cache(dd.cache)) as cache:
        assert next(cache.iter_records('attachments'))['availability'] == 'available'


def test_retained_auxiliary_monitor_skips_source_alias_rows_until_change(collector, capsys, monkeypatch):
    from chatstore.adapters.whatsapp import aliases
    from chatstore.sync_session import SyncSession

    root, dd, _ = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    original = aliases.collect
    reads = []
    def collect_rows(sources):
        reads.append(1)
        return original(sources)
    monkeypatch.setattr(aliases, 'collect', collect_rows)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        auxiliary = session.native_auxiliary(cfg)
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache, auxiliary_session=auxiliary)
            assert result is not None and result.exit_code == 0
            return result.run['snapshot']['prepared_native']['capture']
        first = poll()
        assert len(reads) == 1
        second = poll()
        assert len(reads) == 1
        mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
        with NativeMirror(mirror_path) as mirror:
            assert mirror.conn.execute('SELECT point FROM cs_native_wa_aliases').fetchone()[0] == first['token']
            assert mirror.conn.execute('SELECT point FROM cs_native_wa_alias_state').fetchone()[0] == second['token']
        with closing(sqlite3.connect(root / 'LID.sqlite')) as writer:
            writer.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+1 555 333 4444' WHERE Z_PK=1")
            writer.commit()
        changed = poll()
        assert len(reads) == 2
        with NativeMirror(mirror_path) as mirror:
            assert mirror.conn.execute('SELECT point FROM cs_native_wa_aliases').fetchone()[0] == changed['token']
        auxiliary.close()  # Simulate restart: the durable payload alone cannot authorize reuse.
        poll()
        assert len(reads) == 3
    assert auxiliary._monitor is None


def test_clean_prepared_poll_skips_header_materialization_and_preserves_freshness(collector, capsys, monkeypatch):
    from chatstore.adapters.whatsapp import aliases
    from chatstore.canonical.records import restamp
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    extraction, reads = [], []
    original_extract, original_read = WhatsAppAdapter.extract, aliases.read
    def extract(*args, **kwargs):
        extraction.append(1)
        return original_extract(*args, **kwargs)
    def read(*args, **kwargs):
        reads.append(1)
        return original_read(*args, **kwargs)
    monkeypatch.setattr(WhatsAppAdapter, 'extract', extract)
    monkeypatch.setattr(aliases, 'read', read)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                                     auxiliary_session=session.native_auxiliary(cfg))
            assert result is not None and result.exit_code == 0, result
            return result.run
        poll()
        alias = next(cache.iter_records('aliases'))
        original_checkpoint = cache.get_checkpoint('whatsapp', ident.scope_for('whatsapp'), 'adapter')
        clean = poll()
        assert clean['counts']['prepared_coverage_reused'] == 1
        assert clean['counts']['written'] == {'inserted': 0, 'updated': 0, 'unchanged': 0}
        assert len(extraction) == len(reads) == 1
        assert cache.get_checkpoint('whatsapp', ident.scope_for('whatsapp'), 'adapter') == original_checkpoint
        assert cache.observations_of(alias['urn'])[0]['sync_run'] != alias['sync_run']
        assert cache.observations_of(alias['urn'])[0]['present'] == 'present'
        writer.execute("UPDATE ZWACHATSESSION SET ZPARTNERNAME='new source chat title' WHERE Z_PK=1")
        writer.commit()
        changed = poll()
        assert not changed['counts'].get('prepared_coverage_reused')
        assert len(extraction) == len(reads) == 2
        chat = next(row for row in cache.iter_records('chats') if row['observed_name'] == 'new source chat title')
        with cache.write():
            cache.upsert(restamp(dict(chat, observed_name='local cache drift')))
        recovered = poll()
        assert recovered['mode'] == 'full_reconcile'
        assert not recovered['counts'].get('prepared_coverage_reused')
        assert len(extraction) == len(reads) == 3
        assert cache.get(chat['urn'])['observed_name'] == 'new source chat title'


@pytest.mark.parametrize(('mutation', 'reuse'), [
    ("UPDATE ZWAMESSAGE SET ZTEXT='header-safe edit' WHERE Z_PK=1", True),
    (("INSERT INTO ZWAMESSAGE(Z_PK,ZCHATSESSION,ZSTANZAID,ZTEXT,ZFROMJID,ZTOJID,ZISFROMME,ZMESSAGETYPE,ZMESSAGEDATE,ZSORT) "
      "VALUES(999,1,'NEW-STANZA','new known sender','15550001111@s.whatsapp.net','15550009999@s.whatsapp.net',0,0,800000000,999)"), True),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=2', True),
    ("UPDATE ZWAMESSAGE SET ZFROMJID='new-sender@lid' WHERE Z_PK=1", False),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=1', False),
    ("UPDATE ZWACHATSESSION SET ZPARTNERNAME='header changed' WHERE Z_PK=1", False),
])
def test_message_capture_reuses_only_proven_unchanged_headers(collector, capsys, monkeypatch, mutation, reuse):
    from chatstore.adapters.whatsapp import aliases
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    reads = []
    original = aliases.read
    def read(*args, **kwargs):
        reads.append(1)
        return original(*args, **kwargs)
    monkeypatch.setattr(aliases, 'read', read)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                                    auxiliary_session=session.native_auxiliary(cfg))
            assert result is not None and result.exit_code == 0, result
            return result.run
        poll()
        header = next(cache.iter_records('aliases'))
        writer.execute(mutation)
        writer.commit()
        changed = poll()
        assert bool(changed['counts'].get('prepared_headers_reused')) is reuse
        assert len(reads) == (1 if reuse else 2)
        if reuse:
            assert not any(key in changed['counts'] for key in ('identities', 'aliases', 'chats', 'chat_memberships'))
            assert changed['counts']['written']['unchanged'] == (1 if mutation.startswith('DELETE') else 0)
            assert cache.get(header['urn'])['revision_digest'] == header['revision_digest']
            evidence = cache.observations_of(header['urn'])[0]
            assert evidence['present'] == 'present'
            observed_run = cache.get(evidence['sync_run'])
            assert observed_run['snapshot']['prepared_native']['capture']['token'] == changed['snapshot']['prepared_native']['capture']['token']
        assert poll()['counts']['prepared_coverage_reused'] == 1


def test_header_reuse_retries_offline_and_cold_restores_freshness(collector, capsys, monkeypatch, tmp_path):
    from pathlib import Path

    from chatstore.archive.import_ import import_archive
    from chatstore.archive.incremental import export_changes
    from chatstore.config import Identity
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    writer.execute('UPDATE ZWAMESSAGE SET ZPARENTMESSAGE=1 WHERE Z_PK=8')
    writer.commit()
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            return collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                                   auxiliary_session=session.native_auxiliary(cfg))
        assert poll().exit_code == 0
        header = next(cache.iter_records('aliases'))
        first = next(row for row in cache.iter_records('messages') if row['text'] == 'hello from alice')
        def export():
            return export_changes(cache, dd, cfg, ident, consumer='header-restore', output=dd.exports, password='test-password')
        baseline = export()
        writer.execute("UPDATE ZWAMESSAGE SET ZTEXT='offline header-safe edit' WHERE Z_PK=1")
        writer.commit()
        def interrupt(*args, **kwargs):
            raise RuntimeError('interrupt header-safe capture')
        with monkeypatch.context() as patch:
            patch.setattr(cache, 'upsert_many', interrupt)
            with pytest.raises(RuntimeError, match='interrupt header-safe'):
                poll()
        for name in ('ChatStorage', 'LID'):
            (root / f'{name}.sqlite').rename(root / f'offline-{name}.sqlite')
        retried = poll()
        assert retried.exit_code == 0 and retried.run['counts']['prepared_headers_reused'] == 1
        assert retried.run['counts']['messages'] == 2  # Includes a reply from a different chat.
        reply = next(row for row in cache.iter_records('messages') if row['text'] == 'direct from lid')
        assert reply['reply_to_urn'] == first['urn']
        delta = export()
        expected = cache.observations_of(header['urn'])[0]
        restored = DataDir(tmp_path / 'cold-header-restore')
        restored.create()
        with closing(Cache(restored.cache)) as target:
            for archive in (baseline, delta):
                assert import_archive(target, restored, Identity(), Path(archive['path']), 'test-password').action == 'imported'
            assert target.get(first['urn'])['text'] == 'offline header-safe edit'
            assert len(target.revisions_of(first['urn'])) == 2
            actual = target.observations_of(header['urn'])[0]
            assert (actual['observed_at'], actual['sync_run'], actual['present']) == (
                expected['observed_at'], expected['sync_run'], expected['present'])


@pytest.mark.parametrize('mutation', [
    'PRAGMA user_version=17',
    'UPDATE ZWAZACCOUNT SET ZPHONENUMBER=ZPHONENUMBER WHERE Z_PK=1',
    'CREATE TABLE unrelated_metadata(value TEXT)',
])
def test_auxiliary_commits_with_identical_evidence_reuse_headers(collector, capsys, monkeypatch, mutation):
    from chatstore.adapters.whatsapp import aliases
    from chatstore.sync_session import SyncSession

    root, dd, _ = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    forbidden = False
    original_read = aliases.read
    def read(*args, **kwargs):
        if forbidden:
            pytest.fail('identical auxiliary evidence materialized canonical headers')
        return original_read(*args, **kwargs)
    monkeypatch.setattr(aliases, 'read', read)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                                    auxiliary_session=session.native_auxiliary(cfg))
            assert result is not None and result.exit_code == 0, result
            return result.run
        first = poll()
        header = next(cache.iter_records('aliases'))
        with closing(sqlite3.connect(root / 'LID.sqlite')) as source:
            source.execute(mutation)
            source.commit()
        forbidden = True
        changed = poll()
        forbidden = False
        assert changed['counts']['prepared_coverage_reused'] == 1
        assert changed['counts']['written'] == {'inserted': 0, 'updated': 0, 'unchanged': 0}
        before = first['snapshot']['prepared_native']['capture']
        after = changed['snapshot']['prepared_native']['capture']
        assert before['token'] != after['token']
        assert before['context']['whatsapp_alias_evidence_point'] == after['context']['whatsapp_alias_evidence_point']
        assert cache.observations_of(header['urn'])[0]['sync_run'] != header['sync_run']
        with closing(sqlite3.connect(root / 'LID.sqlite')) as source:
            source.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+15550008888' WHERE Z_PK=1")
            source.commit()
        actual_change = poll()
        assert not actual_change['counts'].get('prepared_coverage_reused')
        assert actual_change['counts']['aliases'] > 0


@pytest.mark.parametrize('mutation', [
    "UPDATE ZWAGROUPMEMBERSCHANGE SET ZMEMBERJIDS='new@s.whatsapp.net' WHERE Z_PK=1",
    "UPDATE ZWAGROUPMEMBERSCHANGE SET ZCHANGEDATE=700000091 WHERE Z_PK=1",
    'DELETE FROM ZWAGROUPMEMBERSCHANGE WHERE Z_PK=1',
])
@pytest.mark.parametrize('duplicate', [False, True])
def test_group_event_changes_skip_unrelated_headers_and_preserve_history(collector, capsys, monkeypatch, mutation, duplicate):
    from chatstore.adapters.whatsapp import aliases
    from chatstore.archive.import_ import import_archive
    from chatstore.archive.incremental import export_changes
    from chatstore.config import Identity
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    writer.executemany('INSERT INTO ZWAGROUPMEMBERSCHANGE VALUES(?,?,?,?,?)',
        ((100 + i, 5, 800000000 + i, 'unrelated@g.us', 'member@s.whatsapp.net') for i in range(1000)))
    # Same canonical key after normalization: deleting one native owner must not
    # withdraw the surviving event, and extraction must preserve native row order.
    if duplicate:
        writer.execute("INSERT INTO ZWAGROUPMEMBERSCHANGE SELECT 2,ZCHANGETYPE,ZCHANGEDATE,upper(ZGROUPJID),'duplicate@s.whatsapp.net' FROM ZWAGROUPMEMBERSCHANGE WHERE Z_PK=1")
    writer.commit()
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    forbid_headers = False
    original = aliases.read
    def read(*args, **kwargs):
        if forbid_headers:
            pytest.fail('selected group events rebuilt unrelated headers')
        return original(*args, **kwargs)
    monkeypatch.setattr(aliases, 'read', read)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                                    auxiliary_session=session.native_auxiliary(cfg))
            assert result is not None and result.exit_code == 0, result
            return result.run
        first = poll()
        before = {row['urn']: row for row in cache.iter_records('events')}
        def export():
            return export_changes(cache, dd, cfg, ident, consumer='events', output=dd.exports, password='test-password')
        baseline = export()
        writer.execute(mutation)
        writer.commit()
        forbid_headers = True
        changed = poll()
        assert changed['counts']['prepared_headers_reused'] == 1
        assert changed['counts'].get('messages', 0) == 0
        assert changed['counts'].get('events', 0) <= 2
        after = {row['urn']: row for row in cache.iter_records('events')}
        assert len(after) >= len(before)
        unchanged = next(row for row in before.values() if row['target_native'] == 'unrelated@g.us')
        observation = cache.observations_of(unchanged['urn'])[0]
        assert observation['sync_run'] != unchanged['sync_run']
        assert observation['present'] == 'present'
        expected_absent = int(not duplicate and ('DELETE' in mutation or 'ZCHANGEDATE=' in mutation))
        assert changed['counts'].get('absent_from_source:withdrawn', 0) == expected_absent
        delta = export()
        restored = DataDir(dd.root.parent / 'event-restore')
        restored.create()
        with closing(Cache(restored.cache)) as target:
            for batch in (baseline, delta):
                assert import_archive(target, restored, Identity(), Path(batch['path']), 'test-password').action == 'imported'
            for urn, row in after.items():
                assert target.get(urn)['revision_digest'] == row['revision_digest']
                assert {r['revision_digest'] for r in target.revisions_of(urn)} == {r['revision_digest'] for r in cache.revisions_of(urn)}
                assert [(o['native_table'], o['present']) for o in target.observations_of(urn)] == [(o['native_table'], o['present']) for o in cache.observations_of(urn)]
        assert first['counts']['events'] >= 1001


def test_selected_group_event_retries_from_retained_point_after_source_expiry(collector, capsys, monkeypatch):
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                                    auxiliary_session=session.native_auxiliary(cfg))
            assert result is not None and result.exit_code == 0, result
            return result.run
        poll()
        writer.execute("UPDATE ZWAGROUPMEMBERSCHANGE SET ZMEMBERJIDS='offline@s.whatsapp.net' WHERE Z_PK=1")
        writer.commit()
        def interrupt(*args, **kwargs):
            raise RuntimeError('interrupt selected event canonical write')
        with monkeypatch.context() as patch:
            patch.setattr(cache, 'upsert_many', interrupt)
            with pytest.raises(RuntimeError, match='interrupt selected event'):
                poll()
        mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
        with NativeMirror(mirror_path) as mirror:
            pending = mirror.pending()
            assert pending is not None
            assert mirror.conn.execute('SELECT point FROM cs_wa_event_state').fetchone()[0] == pending.token
        for name in ('ChatStorage', 'LID'):
            (root / f'{name}.sqlite').rename(root / f'expired-{name}.sqlite')
        retried = poll()
        assert retried['snapshot']['prepared_native']['capture']['token'] == pending.token
        assert retried['counts']['prepared_headers_reused'] == 1
        assert retried['counts']['events'] == 1
        from chatstore.identity import urn as U
        expected = U.derive_urn('whatsapp', ident.scope_for('whatsapp'), 'identity', 'offline@s.whatsapp.net')
        assert any(expected in row['payload'].get('member_identity_urns', []) for row in cache.iter_records('events'))
        with NativeMirror(mirror_path) as mirror:
            assert mirror.pending() is None


def test_failed_native_event_preparation_rolls_back_ownership_with_source_point(collector, capsys, monkeypatch):
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        auxiliary = session.native_auxiliary(cfg)
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache, auxiliary_session=auxiliary)
            assert result is not None and result.exit_code == 0, result
            return result.run
        first = poll()['snapshot']['prepared_native']['capture']
        mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
        with NativeMirror(mirror_path) as mirror:
            before = list(map(tuple, mirror.conn.execute('SELECT * FROM cs_wa_event_keys ORDER BY row_id')))
        writer.execute('UPDATE ZWAGROUPMEMBERSCHANGE SET ZCHANGEDATE=700000091 WHERE Z_PK=1')
        writer.commit()
        original = auxiliary.prepare
        def reject(*args, **kwargs):
            original(*args, **kwargs)
            raise RuntimeError('reject after event ownership preparation')
        with monkeypatch.context() as patch:
            patch.setattr(auxiliary, 'prepare', reject)
            with pytest.raises(RuntimeError, match='reject after event ownership'):
                poll()
        with NativeMirror(mirror_path) as mirror:
            assert mirror.pending() is None
            assert mirror.continuation().token == first['token']
            assert mirror.conn.execute('SELECT point FROM cs_wa_event_state').fetchone()[0] == first['token']
            assert list(map(tuple, mirror.conn.execute('SELECT * FROM cs_wa_event_keys ORDER BY row_id'))) == before
            assert not mirror.conn.execute('SELECT 1 FROM cs_wa_event_changes').fetchone()
        changed = poll()
        assert changed['counts']['events'] == 1
        assert changed['counts']['prepared_headers_reused'] == 1
        assert changed['counts']['absent_from_source:withdrawn'] == 1
