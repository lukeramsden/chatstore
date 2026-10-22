import sqlite3
import sys
from contextlib import closing

import pytest

from chatstore.adapters.base import ExtractStats
from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.cache import Cache
from chatstore.canonical.records import restamp
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.sync import run_sync
from chatstore.sync_session import SyncSession
from tests.fixtures.synthetic import build_messages, build_whatsapp

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='live macOS source monitoring')


@pytest.fixture(params=['whatsapp', 'messages'])
def source(request, tmp_path):
    name = request.param
    root = (build_whatsapp if name == 'whatsapp' else build_messages)(tmp_path / name)
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cfg = Config(source_paths={name: str(root)})
    ident = Identity()
    ident.ensure_scope(name)
    cache = Cache(dd.cache)
    session = SyncSession()
    adapter = WhatsAppAdapter() if name == 'whatsapp' else MessagesAdapter()

    def sync(**kwargs):
        return run_sync(adapter, dd, cfg, ident, cache, session=session, **kwargs)

    # First capture discovers media dependencies; the next stable reconciliation
    # establishes their complete watch coverage before any history can be skipped.
    for _ in range(4):
        result = sync()
        assert result.exit_code == 0, result.run
        if result.run['counts'].get('monitored_coverage_reused'):
            break
    else:
        pytest.fail(f'no stable monitor baseline: {session._monitor.failure}')
    yield adapter, root, cache, session, sync
    session.close()
    cache.close()


def test_clean_poll_skips_snapshot_extraction_and_preserves_exact_sightings(source, monkeypatch):
    adapter, _, cache, _, sync = source
    before = cache.conn.execute('SELECT max(seq) FROM changes').fetchone()[0]

    def forbidden(*args, **kwargs):
        raise AssertionError('unchanged poll read native history')

    monkeypatch.setattr(type(adapter), 'snapshot', forbidden)
    monkeypatch.setattr(type(adapter), 'extract', forbidden)
    monkeypatch.setattr(cache, 'coverage', forbidden)
    # Changing loaded adapter code invalidates the signature, so establish the
    # signatures for this instrumentation without changing the accepted coverage.
    session = source[3]
    # Patch the signature lookup independently; this is a read-work assertion,
    # while policy invalidation is covered separately below.
    monkeypatch.setattr('chatstore.sync_session.code_signature', lambda *args: session._key[3])
    result = sync()
    assert result.exit_code == 0
    assert result.run['counts']['monitored_coverage_reused'] == 1
    changes = list(cache.conn.execute('SELECT operation,data FROM changes WHERE seq>?', (before,)))
    assert sum(row[0] == 'record_refresh' for row in changes) == 1
    assert sum(row[0] == 'observation_refresh' for row in changes) == 1
    assert result.run['snapshot'] is None


def test_old_edit_and_local_divergence_force_reconciliation(source):
    adapter, root, cache, _, sync = source
    db = root / ('ChatStorage.sqlite' if adapter.source == 'whatsapp' else 'chat.db')
    with sqlite3.connect(db) as writer:
        writer.execute("UPDATE ZWAMESSAGE SET ZTEXT='old changed' WHERE Z_PK=1" if adapter.source == 'whatsapp'
                       else "UPDATE message SET text='old changed' WHERE ROWID=1")
    result = sync()
    assert not result.run['counts'].get('monitored_coverage_reused')
    assert result.run['counts']['canonical_units_rebuilt'] == 1
    records = list(cache.iter_records('messages'))
    changed = next(r for r in records if r.get('text') == 'old changed')
    with cache.write():
        cache.upsert(restamp(dict(changed, text='local divergent head')), observed_at=1)
    result = sync()
    assert not result.run['counts'].get('monitored_coverage_reused')
    assert cache.get(changed['urn'])['text'] == 'old changed'


def test_full_mode_bypasses_monitored_reuse(source):
    _, _, _, _, sync = source
    result = sync(full=True)
    assert result.exit_code == 0
    assert not result.run['counts'].get('monitored_coverage_reused')
    assert result.run['counts']['canonical_units_rebuilt'] > 0


def test_open_media_write_is_not_hidden_by_unchanged_database(source):
    import hashlib
    import os

    adapter, root, cache, _, sync = source
    media = root / ('Message/Media/x/pic.jpg' if adapter.source == 'whatsapp' else 'Attachments/ab/01/IMG_0001.HEIC')
    with media.open('r+b', buffering=0) as writer:
        writer.write(b'changed!')
        os.fsync(writer.fileno())
        result = sync()
        assert result.exit_code == 0
        assert not result.run['counts'].get('monitored_coverage_reused')
        digest = hashlib.sha256(media.read_bytes()).hexdigest()
        assert any(r['blob_sha256'] == digest for r in cache.iter_records('attachments'))


def test_closed_monitor_and_policy_change_force_reconciliation(source, monkeypatch):
    _, _, _, session, sync = source
    session.close()
    result = sync()
    assert result.exit_code == 0
    assert not result.run['counts'].get('monitored_coverage_reused')
    for _ in range(3):
        if sync().run['counts'].get('monitored_coverage_reused'):
            break
    else:
        pytest.fail('monitor did not regain stable coverage')
    monkeypatch.setattr('chatstore.sync_session.code_signature', lambda *args: 'changed-policy')
    result = sync()
    assert result.exit_code == 0
    assert not result.run['counts'].get('monitored_coverage_reused')


def test_intervening_cache_writer_cannot_publish_monitored_summary(source):
    _, _, cache, session, sync = source
    changed = False

    def progress(message):
        nonlocal changed
        if message.startswith('extracted ') and not changed:
            changed = True
            r = next(cache.iter_records('messages'))
            other = Cache(cache.path)
            try:
                with other.write():
                    other.upsert(restamp(dict(r, text='concurrent local head')), observed_at=1)
            finally:
                other.close()

    result = sync(full=True, progress=progress)
    assert result.exit_code == 0 and changed
    assert session._summary is None
    result = sync()
    assert not result.run['counts'].get('monitored_coverage_reused')


def test_interrupted_reconciliation_discards_reuse_summary(source, monkeypatch):
    adapter, _, _, session, sync = source
    original = type(adapter).extract

    def interrupted(self, *args, **kwargs):
        iterator = original(self, *args, **kwargs)
        try:
            yield next(iterator)
            raise RuntimeError('interrupted capture')
        finally:
            iterator.close()

    with monkeypatch.context() as patch:
        patch.setattr(type(adapter), 'extract', interrupted)
        with pytest.raises(RuntimeError, match='interrupted capture'):
            sync(full=True)
        assert session._summary is None
    result = sync()
    assert result.exit_code == 0
    assert not result.run['counts'].get('monitored_coverage_reused')


def test_other_source_capture_does_not_force_this_source_to_rescan(source):
    adapter, root, cache, _, sync = source
    other = 'messages' if adapter.source == 'whatsapp' else 'whatsapp'
    other_root = (build_messages if other == 'messages' else build_whatsapp)(root.parent / 'other-source')
    other_adapter = MessagesAdapter() if other == 'messages' else WhatsAppAdapter()
    identity = Identity()
    identity.ensure_scope(other)
    dd = DataDir(cache.path.parent)
    with SyncSession() as other_session:
        result = run_sync(other_adapter, dd, Config(source_paths={other: str(other_root)}), identity,
                          cache, session=other_session)
        assert result.exit_code == 0
        assert result.run['counts']['written']['inserted'] > 0
    result = sync()
    assert result.exit_code == 0
    assert result.run['counts']['monitored_coverage_reused'] == 1
    assert result.run['snapshot'] is None


def test_auxiliary_database_created_before_capture_gets_sqlite_version_coverage(tmp_path):
    root = build_whatsapp(tmp_path / 'whatsapp').resolve()
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cfg = Config(source_paths={'whatsapp': str(root)})
    ident = Identity()
    scope, _ = ident.ensure_scope('whatsapp')
    with closing(Cache(dd.cache)) as cache, SyncSession() as session:
        session.prepare(WhatsAppAdapter(), cfg, cache, scope)
        auxiliary = root / 'ContactsV2.sqlite'
        with closing(sqlite3.connect(auxiliary)) as writer:
            writer.execute('PRAGMA journal_mode=WAL')
            writer.execute('CREATE TABLE context(value TEXT)')
            writer.commit()
            session.begin(cache, ExtractStats())
            assert str(auxiliary) in dict(session._before.database_versions)
            assert session._monitor.accept_capture(session._before)
            writer.execute("INSERT INTO context VALUES('changed through WAL')")
            writer.commit()
            assert not session._monitor.unchanged()
            assert auxiliary in session._monitor.dirty_paths()
