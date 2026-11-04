import hashlib
import json
import os
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
    # Only the new alias pair and its two identities are re-extracted.
    assert auxiliary['counts'].get('messages', 0) == 0 and auxiliary['counts']['aliases'] == 1
    assert auxiliary['counts']['identities'] == 2 and auxiliary['counts']['prepared_headers_reused'] == 1
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


def test_file_path_transition_is_captured_by_native_delta(collector, capsys):
    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    initial = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    token = initial['snapshot']['prepared_native']['capture']['token']
    writer.execute("UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH='Media/x/pic.jpg' WHERE Z_PK=1")
    writer.commit()
    delta = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert delta['snapshot']['prepared_native']['capture']['parent'] == token
    assert delta['mode'] == 'incremental' and delta['counts']['messages'] == 1
    with closing(Cache(dd.cache)) as cache:
        assert next(cache.iter_records('attachments'))['availability'] == 'available'


def test_auxiliary_alias_rows_are_skipped_until_change_across_restarts(collector, capsys, monkeypatch):
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
        auxiliary.close()  # Simulate restart: the persisted file fingerprint authorizes reuse.
        poll()
        assert len(reads) == 2
        auxiliary.close()
        with closing(sqlite3.connect(root / 'LID.sqlite')) as writer:
            writer.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+1 555 333 5555' WHERE Z_PK=1")
            writer.commit()
        restarted = poll()
        assert len(reads) == 3
        with NativeMirror(mirror_path) as mirror:
            assert mirror.conn.execute('SELECT point FROM cs_native_wa_aliases').fetchone()[0] == restarted['token']
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


def assert_matches_full_extraction(cache, dd, cfg, ident):
    """Selective output is correct iff a full re-extraction finds nothing to change."""
    full = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache, full=True)
    assert full is not None and full.exit_code == 0, full.run
    counts = full.run['counts']
    assert counts['written']['inserted'] == 0 and counts['written']['updated'] == 0, counts['written']
    assert not any(key.startswith('absent_from_source') for key in counts), counts


@pytest.mark.parametrize(('mutation', 'headers'), [
    ("UPDATE ZWAMESSAGE SET ZTEXT='header-safe edit' WHERE Z_PK=1", False),
    (("INSERT INTO ZWAMESSAGE(Z_PK,ZCHATSESSION,ZSTANZAID,ZTEXT,ZFROMJID,ZTOJID,ZISFROMME,ZMESSAGETYPE,ZMESSAGEDATE,ZSORT) "
      "VALUES(999,1,'NEW-STANZA','new known sender','15550001111@s.whatsapp.net','15550009999@s.whatsapp.net',0,0,800000000,999)"), False),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=2', False),
    # These change header records: only their keys are withdrawn and re-extracted.
    ("UPDATE ZWAMESSAGE SET ZFROMJID='new-sender@lid' WHERE Z_PK=1", True),
    ('DELETE FROM ZWAMESSAGE WHERE Z_PK=1', True),
    ("UPDATE ZWACHATSESSION SET ZPARTNERNAME='header changed' WHERE Z_PK=1", True),
])
def test_message_capture_reextracts_only_changed_headers(collector, capsys, mutation, headers):
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
        header = next(cache.iter_records('aliases'))
        total = sum(1 for kind in ('identities', 'chats', 'chat_memberships', 'aliases') for _ in cache.iter_records(kind))
        writer.execute(mutation)
        writer.commit()
        changed = poll()
        assert changed['counts']['prepared_headers_reused'] == 1
        emitted = sum(changed['counts'].get(kind, 0) for kind in ('identities', 'aliases', 'chats', 'chat_memberships'))
        if headers:
            assert 0 < emitted < total
        else:
            assert emitted == 0
            assert changed['counts']['written']['unchanged'] == (1 if mutation.startswith('DELETE') else 0)
        assert cache.get(header['urn'])['revision_digest'] == header['revision_digest']
        evidence = cache.observations_of(header['urn'])[0]
        assert evidence['present'] == 'present'
        observed_run = cache.get(evidence['sync_run'])
        assert observed_run['snapshot']['prepared_native']['capture']['token'] == changed['snapshot']['prepared_native']['capture']['token']
        assert poll()['counts']['prepared_coverage_reused'] == 1
        assert_matches_full_extraction(cache, dd, cfg, ident)


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


@pytest.mark.parametrize('metadata', ['event', 'chat'])
def test_selected_header_retries_from_retained_point_after_source_expiry(collector, capsys, monkeypatch, metadata):
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
        sql = "UPDATE ZWAGROUPMEMBERSCHANGE SET ZMEMBERJIDS='offline@s.whatsapp.net' WHERE Z_PK=1" if metadata == 'event' else 'UPDATE ZWACHATSESSION SET ZARCHIVED=1 WHERE Z_PK=1'
        writer.execute(sql)
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
        assert retried['counts']['events' if metadata == 'event' else 'chats'] == 1
        from chatstore.identity import urn as U
        expected = U.derive_urn('whatsapp', ident.scope_for('whatsapp'), 'identity', 'offline@s.whatsapp.net')
        if metadata == 'event':
            assert any(expected in row['payload'].get('member_identity_urns', []) for row in cache.iter_records('events'))
        else:
            assert next(row for row in cache.iter_records('chats') if row['native_key'] == '15550001111@s.whatsapp.net')['is_archived']
        with NativeMirror(mirror_path) as mirror:
            assert mirror.pending() is None


@pytest.mark.parametrize('metadata', ['event', 'chat'])
def test_failed_native_header_preparation_rolls_back_ownership_with_source_point(collector, capsys, monkeypatch, metadata):
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
        tables = (('cs_wa_event_keys', 'row_id'), ('cs_wa_event_state', 'id'), ('cs_wa_event_changes', 'event_key')) if metadata == 'event' else (
            ('cs_wa_header_fp', 'native_table,native_key'), ('cs_wa_header_state', 'id'), ('cs_wa_header_changes', 'native_table,native_key'))
        def snapshot():
            with NativeMirror(mirror_path) as mirror:
                return {table: list(map(tuple, mirror.conn.execute(f'SELECT * FROM {table} ORDER BY {order}'))) for table, order in tables}
        before = snapshot()
        assert before[tables[1][0]][0][1] == first['token']
        sql = 'UPDATE ZWAGROUPMEMBERSCHANGE SET ZCHANGEDATE=700000091 WHERE Z_PK=1' if metadata == 'event' else "UPDATE ZWACHATSESSION SET ZPARTNERNAME='retried group title' WHERE Z_PK=2"
        writer.execute(sql)
        writer.commit()
        with monkeypatch.context() as patch:
            if metadata == 'event':
                original = auxiliary.prepare
                def reject(*args, **kwargs):
                    original(*args, **kwargs)
                    raise RuntimeError('reject after header preparation')
                patch.setattr(auxiliary, 'prepare', reject)
            else:
                from types import SimpleNamespace

                from chatstore import native_whatsapp
                from chatstore.adapters.whatsapp import headers
                def reject_headers(conn, *, full):
                    headers.prepare(conn, full=full)
                    raise RuntimeError('reject after header preparation')
                # Not a policy module function, so the extraction policy is unchanged.
                patch.setattr(native_whatsapp, 'headers', SimpleNamespace(prepare=reject_headers))
            with pytest.raises(RuntimeError, match='reject after header preparation'):
                poll()
        with NativeMirror(mirror_path) as mirror:
            assert mirror.pending() is None
            assert mirror.continuation().token == first['token']
        assert snapshot() == before
        changed = poll()
        assert changed['counts']['events' if metadata == 'event' else 'chats'] == 1
        assert changed['counts']['prepared_headers_reused'] == 1
        assert changed['counts'].get('absent_from_source:withdrawn', 0) == (1 if metadata == 'event' else 0)


@pytest.mark.parametrize('duplicate', [False, True])
@pytest.mark.parametrize('mutation', [
    'UPDATE ZWACHATSESSION SET ZARCHIVED=1-ZARCHIVED WHERE Z_PK=1',
    'UPDATE ZWACHATSESSION SET ZREMOVED=1 WHERE Z_PK=2',
    "UPDATE ZWACHATSESSION SET ZPARTNERNAME='edited group title' WHERE Z_PK=2",
])
def test_chat_metadata_selects_key_owners_and_retains_unrelated_headers(collector, capsys, monkeypatch, duplicate, mutation):
    from chatstore.archive.import_ import import_archive
    from chatstore.archive.incremental import export_changes
    from chatstore.config import Identity
    from chatstore.sync_session import SyncSession

    root, dd, writer = collector
    columns = [row[1] for row in writer.execute('PRAGMA table_info(ZWACHATSESSION)')]
    original = dict(zip(columns, writer.execute('SELECT * FROM ZWACHATSESSION WHERE Z_PK=2').fetchone(), strict=True))
    def insert(row):
        writer.execute(f"INSERT INTO ZWACHATSESSION({','.join(columns)}) VALUES({','.join('?' for _ in columns)})", [row[c] for c in columns])
    for i in range(100):
        insert(dict(original, Z_PK=100+i, ZCONTACTJID=f'unrelated-{i}@g.us'))
    if duplicate:
        insert(dict(original, Z_PK=20, ZCONTACTJID=original['ZCONTACTJID'].upper(), ZPARTNERNAME='duplicate owner'))
    writer.execute("INSERT INTO ZWAGROUPINFO VALUES(2,2,690000001,'creator@s.whatsapp.net','owner@s.whatsapp.net')")
    writer.commit()
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache, auxiliary_session=session.native_auxiliary(cfg))
            assert result is not None and result.exit_code == 0, result
            return result.run
        poll()
        unrelated = next(row for row in cache.iter_records('chats') if row['native_key'] == 'unrelated-0@g.us')
        def export():
            return export_changes(cache, dd, cfg, ident, consumer='chats', output=dd.exports, password='test-password')
        baseline = export()
        writer.execute(mutation)
        writer.commit()
        changed = poll()
        assert changed['counts']['prepared_headers_reused'] == 1
        assert changed['counts']['chats'] == (2 if duplicate and 'Z_PK=2' in mutation else 1)
        assert not any(changed['counts'].get(kind) for kind in ('messages', 'identities', 'aliases', 'chat_memberships', 'events'))
        assert changed['counts'].get('absent_from_source:withdrawn', 0) == 0
        assert cache.observations_of(unrelated['urn'])[0]['sync_run'] != unrelated['sync_run']
        from chatstore.adapters.base import Emit, ExtractStats
        from chatstore.prepared_sync import PreparedSync
        with NativeMirror(next((dd.root / 'native').glob('*.sqlite3'))) as mirror:
            snap = PreparedSync(mirror, mirror.continuation(), 'ChatStorage').snapshot()
            full = {item.record['urn']: item.record['revision_digest']
                    for item in WhatsAppAdapter().extract(snap, ident.scope_for('whatsapp'), cfg, ExtractStats(), None, full=True)
                    if isinstance(item, Emit) and item.record['entity'] == 'chats'}
        assert {row['urn']: row['revision_digest'] for row in cache.iter_records('chats')} == full
        delta = export()
        for name in ('ChatStorage', 'LID'):
            (root / f'{name}.sqlite').rename(root / f'expired-{name}.sqlite')
        restored = DataDir(dd.root.parent / 'chat-restore')
        restored.create()
        with closing(Cache(restored.cache)) as target:
            for batch in (baseline, delta):
                assert import_archive(target, restored, Identity(), Path(batch['path']), 'test-password').action == 'imported'
            for row in cache.iter_records('chats'):
                assert target.get(row['urn'])['revision_digest'] == row['revision_digest']
                assert {r['revision_digest'] for r in target.revisions_of(row['urn'])} == {r['revision_digest'] for r in cache.revisions_of(row['urn'])}


def test_one_shot_cli_with_downloaded_media_is_selective_and_cwd_independent(collector, capsys, monkeypatch, tmp_path):
    root, dd, writer = collector
    picture = root / 'Message/Media/x/pic.jpg'
    writer.execute("UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH='Media/x/pic.jpg' WHERE Z_PK=1")
    writer.commit()
    mirror_path = None

    def media_state():
        with NativeMirror(mirror_path) as mirror:
            changes = [row[0] for row in mirror.conn.execute('SELECT row_id FROM cs_native_wa_media_changes')]
            return changes, mirror.conn.execute('SELECT full FROM cs_native_wa_media_coverage').fetchone()[0]

    monkeypatch.chdir('/')
    cli(capsys, 'init', '--whatsapp-path', str(root))
    initial = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    capture = initial['snapshot']['prepared_native']['capture']
    assert capture['baseline'] and capture['context']['whatsapp_media'] == {'format': 1}
    descriptors = len(os.listdir('/dev/fd'))
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    monkeypatch.chdir(tmp_path)
    clean = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    clean_capture = clean['snapshot']['prepared_native']['capture']
    assert clean_capture['parent'] == capture['token'] and not clean_capture['baseline']
    assert clean_capture['policy_tag'] == capture['policy_tag']
    assert clean['counts']['prepared_coverage_reused'] == 1
    assert media_state() == ([], 0)  # No file bytes were read.
    writer.execute("UPDATE ZWAMESSAGE SET ZTEXT='old edit with downloaded media present' WHERE Z_PK=1")
    writer.commit()
    edited = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert edited['mode'] == 'incremental' and edited['counts']['messages'] == 1
    assert media_state() == ([], 0)
    picture.write_bytes(b'rewritten in place between one-shot runs')
    rewritten = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert rewritten['mode'] == 'incremental' and rewritten['counts']['messages'] == 1
    assert media_state() == ([1], 0)
    assert len(os.listdir('/dev/fd')) == descriptors  # No watcher state survives a run.
    full = cli(capsys, 'sync', '--source', 'whatsapp', '--mode', 'full')[0]
    assert full['mode'] == 'full_reconcile'
    assert media_state() == ([], 1)  # --mode full re-hashes every file, not just dirty ones.
    after_full = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert after_full['counts']['prepared_coverage_reused'] == 1 and media_state() == ([], 0)
    with closing(Cache(dd.cache)) as cache:
        assert next(cache.iter_records('attachments'))['blob_sha256'] == hashlib.sha256(picture.read_bytes()).hexdigest()


@pytest.mark.racy_window
def test_recently_written_auxiliary_database_is_reread_after_restart(collector, capsys, monkeypatch):
    from chatstore.adapters.whatsapp import aliases
    from chatstore.adapters.whatsapp.auxiliary import AuxiliarySession

    root, dd, _ = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    from chatstore.adapters import media_ledger

    original, reads = aliases.collect, []
    monkeypatch.setattr(aliases, 'collect', lambda sources: reads.append(1) or original(sources))
    # Every scan happens 1s after LID.sqlite's last change, inside the racy window.
    written = max(os.stat(root / name).st_ctime_ns for name in ('LID.sqlite',))
    monkeypatch.setattr(media_ledger.time, 'time_ns', lambda: written + 10**9)
    with closing(Cache(dd.cache)) as cache:
        for _ in range(2):  # A new session per poll stands in for a new process.
            session = AuxiliarySession(cfg.source_root('whatsapp'))
            try:
                assert collect_native(WhatsAppAdapter(), dd, cfg, ident, cache, auxiliary_session=session).exit_code == 0
            finally:
                session.close()
    # A racy fingerprint proves nothing, so the restarted session re-reads the rows.
    assert len(reads) == 2


def test_new_message_with_chat_bookkeeping_update_reuses_headers(collector, capsys):
    root, _, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cli(capsys, 'sync', '--source', 'whatsapp')
    columns = [row[1] for row in writer.execute('PRAGMA table_info(ZWAMESSAGE)')]
    values = dict(zip(columns, writer.execute('SELECT * FROM ZWAMESSAGE WHERE Z_PK=1').fetchone(), strict=True))
    values.update(Z_PK=writer.execute('SELECT max(Z_PK)+1 FROM ZWAMESSAGE').fetchone()[0],
                  ZSORT=writer.execute('SELECT max(ZSORT)+1 FROM ZWAMESSAGE').fetchone()[0],
                  ZSTANZAID='NEW-WITH-CHAT-BOOKKEEPING', ZTEXT='arrives with a chat row update')
    writer.execute(f"INSERT INTO ZWAMESSAGE({','.join(columns)}) VALUES({','.join('?' for _ in columns)})",
                   [values[column] for column in columns])
    # WhatsApp rewrites the chat's bookkeeping columns with every message.
    writer.execute('UPDATE ZWACHATSESSION SET ZLASTMESSAGEDATE=coalesce(ZLASTMESSAGEDATE,0)+1, Z_OPT=coalesce(Z_OPT,0)+1 WHERE Z_PK=?',
                   (values['ZCHATSESSION'],))
    writer.commit()
    result = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert result['mode'] == 'incremental' and result['counts']['messages'] == 1
    assert result['counts'].get('prepared_headers_reused') == 1
    # Bookkeeping columns feed no header payload: no chat, identity or membership is re-emitted.
    assert not any(result['counts'].get(kind) for kind in ('identities', 'chats', 'chat_memberships', 'aliases'))


def test_alias_database_write_during_long_baseline_does_not_reject_capture(collector, capsys, monkeypatch):
    from chatstore.adapters.whatsapp import events

    root, dd, _ = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    original = events.prepare
    def baseline_work_while_whatsapp_writes(conn, *, full):
        # Stands in for minutes of baseline work while WhatsApp updates LID.sqlite.
        with closing(sqlite3.connect(root / 'LID.sqlite')) as lid:
            lid.execute("UPDATE ZWAZACCOUNT SET ZPHONENUMBER='+1 555 777 0000' WHERE Z_PK=1")
            lid.commit()
        return original(conn, full=full)
    monkeypatch.setattr(events, 'prepare', baseline_work_while_whatsapp_writes)
    result = cli(capsys, 'sync', '--source', 'whatsapp')[0]
    assert result['status'] == 'complete'
    assert result['snapshot']['prepared_native']['capture']['baseline']
    with closing(Cache(dd.cache)) as cache:
        assert any('15557770000' in (row.get('address') or '') for row in cache.iter_records('identities'))


HEADER_MUTATIONS = {
    'new_direct_chat_and_message': [
        "INSERT INTO ZWACHATSESSION VALUES (40,1,1,0,0,0,0,NULL,'15550004040@s.whatsapp.net','New Contact',800000000)",
        ("INSERT INTO ZWAMESSAGE(Z_PK,ZCHATSESSION,ZSTANZAID,ZTEXT,ZFROMJID,ZISFROMME,ZMESSAGETYPE,ZMESSAGEDATE,ZSORT) "
         "VALUES(4040,40,'NEW-CHAT-1','hello','15550004040@s.whatsapp.net',0,0,800000001,4040)")],
    'new_group_member': ["INSERT INTO ZWAGROUPMEMBER VALUES (40,1,0,2,'15550004141@s.whatsapp.net','New Member')"],
    'member_leaves_and_promoted': ['UPDATE ZWAGROUPMEMBER SET ZISACTIVE=0 WHERE Z_PK=2', 'UPDATE ZWAGROUPMEMBER SET ZISADMIN=1 WHERE Z_PK=3'],
    'push_name_change': ["INSERT INTO ZWAPROFILEPUSHNAME(ZJID,ZPUSHNAME) VALUES('15550004242@s.whatsapp.net','Pushy')",
                         "UPDATE ZWAPROFILEPUSHNAME SET ZPUSHNAME=ZPUSHNAME||' renamed' WHERE Z_PK=1"],
    'direct_chat_renamed': ["UPDATE ZWACHATSESSION SET ZPARTNERNAME='Alice Renamed' WHERE Z_PK=1"],
    'chat_deleted': ['DELETE FROM ZWAMESSAGE WHERE ZCHATSESSION=3', 'DELETE FROM ZWACHATSESSION WHERE Z_PK=3'],
    'group_created_at': ["UPDATE ZWAGROUPINFO SET ZCREATIONDATE=690000123 WHERE ZCHATSESSION=2"],
    'new_owner_jid': [("INSERT INTO ZWAMESSAGE(Z_PK,ZCHATSESSION,ZSTANZAID,ZTEXT,ZFROMJID,ZTOJID,ZISFROMME,ZMESSAGETYPE,ZMESSAGEDATE,ZSORT) "
                       "VALUES(4343,1,'OWNER-1','to me','15550004343@s.whatsapp.net','15550009998@s.whatsapp.net',0,0,800000002,4343)")],
    'new_alias_pair': ['lid:INSERT INTO ZWAZACCOUNT VALUES (40, \'4040@lid\', \'+1 555 000 4040\')'],
    'alias_removed': ['lid:DELETE FROM ZWAZACCOUNT WHERE Z_PK=1'],
    'alias_becomes_ambiguous': ['lid:INSERT INTO ZWAZACCOUNT VALUES (41, (SELECT ZIDENTIFIER FROM ZWAZACCOUNT WHERE Z_PK=1), \'+1 555 000 4141\')'],
}


@pytest.mark.parametrize('name', sorted(HEADER_MUTATIONS))
def test_header_changes_reextract_only_affected_keys_and_match_full_extraction(collector, capsys, name):
    root, dd, writer = collector
    cli(capsys, 'init', '--whatsapp-path', str(root))
    cfg, ident = load_config(dd), load_identity(dd)
    with closing(Cache(dd.cache)) as cache:
        def poll():
            result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache)
            assert result is not None and result.exit_code in (0, 5), result.run
            return result.run
        poll()
        total = sum(1 for kind in ('identities', 'chats', 'chat_memberships', 'aliases') for _ in cache.iter_records(kind))
        with closing(sqlite3.connect(root / 'LID.sqlite')) as lid:
            for statement in HEADER_MUTATIONS[name]:
                (lid if statement.startswith('lid:') else writer).execute(statement.removeprefix('lid:'))
            lid.commit()
        writer.commit()
        changed = poll()
        assert changed['mode'] == 'incremental'
        assert changed['counts']['prepared_headers_reused'] == 1, changed['counts']
        emitted = sum(changed['counts'].get(kind, 0) for kind in ('identities', 'aliases', 'chats', 'chat_memberships'))
        withdrawn = changed['counts'].get('absent_from_source:withdrawn', 0)
        assert 0 < emitted + withdrawn and emitted < total, (emitted, withdrawn, total)
        assert poll()['counts']['prepared_coverage_reused'] == 1
        assert_matches_full_extraction(cache, dd, cfg, ident)
