import json
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.cache import Cache
from chatstore.cli.app import main
from chatstore.config import load_config, load_identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir
from tests.fixtures.synthetic import build_messages

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS native source coordinator')


@pytest.fixture
def collector(tmp_path, monkeypatch):
    helper = Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        pytest.skip('build development Messages helper first')
    root = build_messages(tmp_path / 'messages', extra_messages=50)
    dd = DataDir(tmp_path / 'data')
    monkeypatch.setenv('CHATSTORE_DATA_DIR', str(dd.root))
    monkeypatch.setenv('CHATSTORE_MESSAGES_DECODER', str(helper))
    with closing(sqlite3.connect(root / 'chat.db')) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('DELETE FROM message_attachment_join')
        writer.execute('DELETE FROM attachment')
        writer.commit()
        yield dd, root, writer


def cli(capsys, *args):
    code = main(['--json', *args])
    result = json.loads(capsys.readouterr().out)
    assert code == 0, result
    return result['data']


def test_cli_native_capture_survives_restart_and_selects_old_changes(collector, capsys):
    dd, root, writer = collector
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    initial = cli(capsys, 'sync', '--source', 'messages')[0]
    assert initial['snapshot']['prepared_native']['capture']['baseline']
    parent = initial['snapshot']['prepared_native']['capture']['token']
    writer.execute("UPDATE message SET text='old edit through production collector' WHERE ROWID=1")
    writer.commit()
    changed = cli(capsys, 'sync', '--source', 'messages')[0]
    assert changed['counts']['messages'] == 1
    assert changed['snapshot']['prepared_native']['capture']['parent'] == parent
    clean = cli(capsys, 'sync', '--source', 'messages')[0]
    assert clean['counts'].get('messages', 0) == 0
    writer.execute('DELETE FROM message WHERE ROWID=1')
    writer.commit()
    deleted = cli(capsys, 'sync', '--source', 'messages')[0]
    assert deleted['counts'].get('messages', 0) == 0
    with closing(Cache(dd.cache)) as cache:
        record = next(row for row in cache.iter_records('messages') if row['text'] == 'old edit through production collector')
        assert cache.observations_of(record['urn'])[0]['present'] == 'absent_from_source'
        assert len(cache.revisions_of(record['urn'])) == 2
    writer.execute('ALTER TABLE message ADD COLUMN added_context TEXT')
    writer.commit()
    rebased = cli(capsys, 'sync', '--source', 'messages')[0]
    assert rebased['snapshot']['prepared_native']['capture']['baseline']
    assert rebased['snapshot']['prepared_native']['capture']['parent'] == deleted['snapshot']['prepared_native']['capture']['token']


def test_failed_canonical_capture_retries_retained_point_without_source(collector, capsys, monkeypatch):
    dd, root, _ = collector
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    def interrupted(*args, **kwargs):
        raise RuntimeError('interrupted before canonical capture')
    with closing(Cache(dd.cache)) as cache:
        monkeypatch.setattr(cache, 'upsert_many', interrupted)
        with pytest.raises(RuntimeError, match='interrupted'):
            collect_native(MessagesAdapter(), dd, load_config(dd), load_identity(dd), cache)
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        pending = mirror.pending()
        assert pending is not None
    (root / 'chat.db').rename(root / 'offline.db')
    restored = cli(capsys, 'sync', '--source', 'messages')[0]
    assert restored['snapshot']['prepared_native']['capture']['token'] == pending.token
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation() == pending


def test_unrelated_native_table_changes_do_not_force_full_history_capture(collector, capsys):
    _, root, writer = collector
    writer.execute('CREATE TABLE unrelated_application_state(value TEXT)')
    writer.execute("INSERT INTO unrelated_application_state VALUES('initial')")
    writer.commit()
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    cli(capsys, 'sync', '--source', 'messages')
    writer.execute("UPDATE unrelated_application_state SET value='changed'")
    writer.commit()
    result = cli(capsys, 'sync', '--source', 'messages')[0]
    assert result['mode'] == 'incremental'
    assert result['counts'].get('messages', 0) == 0
    assert not result['snapshot']['prepared_native']['capture']['baseline']


def test_file_backed_source_uses_ordinary_capture_without_publishing_native_point(collector, capsys):
    dd, root, writer = collector
    writer.execute("INSERT INTO attachment(ROWID,guid,filename) VALUES(1,'attachment-guid','~/Library/Messages/Attachments/file')")
    writer.commit()
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    result = cli(capsys, 'sync', '--source', 'messages')[0]
    assert 'prepared_native' not in result['snapshot']
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation() is None


def test_delta_does_not_query_source_attachment_history(collector, capsys, monkeypatch):
    from chatstore.adapters.source_view import WalSource

    _, root, writer = collector
    writer.executemany('INSERT INTO attachment(ROWID,guid,filename) VALUES(?,?,NULL)',
                       ((row, f'cloud-{row}') for row in range(1, 2001)))
    writer.commit()
    queries = []
    original = WalSource._open
    def traced(self, *args, **kwargs):
        view = original(self, *args, **kwargs)
        view.connection.set_trace_callback(queries.append)
        return view
    monkeypatch.setattr(WalSource, '_open', traced)
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    cli(capsys, 'sync', '--source', 'messages')
    assert any("SELECT 1 FROM attachment WHERE filename>''" in query for query in queries)
    queries.clear()
    result = cli(capsys, 'sync', '--source', 'messages')[0]
    assert result['counts'].get('messages', 0) == 0
    assert not any('FROM attachment' in query for query in queries)


def test_new_file_path_rolls_back_candidate_before_ordinary_capture(collector, capsys):
    dd, root, writer = collector
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    initial = cli(capsys, 'sync', '--source', 'messages')[0]
    token = initial['snapshot']['prepared_native']['capture']['token']
    writer.execute("INSERT INTO attachment(ROWID,guid,filename) VALUES(1,'new-file-guid','~/Library/Messages/Attachments/file')")
    writer.execute('INSERT INTO message_attachment_join(message_id,attachment_id) VALUES(1,1)')
    writer.commit()
    fallback = cli(capsys, 'sync', '--source', 'messages')[0]
    assert 'prepared_native' not in fallback['snapshot']
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation().token == token
        assert mirror.conn.execute('SELECT count(*) FROM attachment').fetchone()[0] == 0
        assert mirror.conn.execute('SELECT point FROM cs_native_page_state').fetchone()[0] == token
    writer.execute('UPDATE attachment SET filename=NULL')
    writer.commit()
    captured = cli(capsys, 'sync', '--source', 'messages')[0]
    assert captured['snapshot']['prepared_native']['capture']['parent'] == token
    assert captured['status'] == 'complete'


def test_private_media_eligibility_lookup_is_bounded_by_index():
    from chatstore.native_collector import MediaCaptureRequired, _validate_text_only

    with closing(sqlite3.connect(':memory:')) as conn:
        conn.execute('CREATE TABLE attachment(filename TEXT)')
        conn.executemany('INSERT INTO attachment VALUES(?)', ((None,) for _ in range(100000)))
        conn.execute('CREATE INDEX cs_route_media_file ON attachment(filename)')
        steps = []
        conn.set_progress_handler(lambda: steps.append(1) or 0, 1)
        _validate_text_only(conn)
        assert len(steps) < 30
        conn.set_progress_handler(None, 0)
        conn.execute("INSERT INTO attachment VALUES('/new-file')")
        with pytest.raises(MediaCaptureRequired):
            _validate_text_only(conn)


def test_session_collector_routes_file_only_changes_without_unrelated_messages(collector, capsys):
    from chatstore.sync_session import SyncSession

    dd, root, writer = collector
    filename = root / 'Attachments' / 'watched.bin'
    filename.write_bytes(b'initial retained bytes')
    writer.execute("INSERT INTO attachment(ROWID,guid,filename,total_bytes) VALUES(1,'watched-guid',?,22)", (str(filename),))
    writer.execute('INSERT INTO message_attachment_join(message_id,attachment_id) VALUES(1,1)')
    writer.commit()
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    cfg, ident = load_config(dd), load_identity(dd)
    with SyncSession() as session, closing(Cache(dd.cache)) as cache:
        watcher = session.native_media(cfg, dd)
        def poll():
            outcome = collect_native(MessagesAdapter(), dd, cfg, ident, cache, media_watch=watcher)
            assert outcome is not None and outcome.exit_code == 0
            return outcome.run
        initial = poll()
        assert initial['snapshot']['prepared_native']['capture']['baseline']
        clean = poll()
        assert clean['counts'].get('messages', 0) == 0
        filename.write_bytes(b'file-only change through collector')
        changed = poll()
        assert changed['counts']['messages'] == 1
        attachment = next(cache.iter_records('attachments'))
        assert attachment['availability'] == 'available'
        assert len(cache.revisions_of(attachment['urn'])) == 2
        filename.unlink()
        missing = poll()
        assert missing['counts']['messages'] == 1
        assert cache.get(attachment['urn'])['availability'] == 'missing'
        assert len(cache.revisions_of(attachment['urn'])) == 3
    assert watcher._monitor is None


@pytest.mark.parametrize('failure', ['mutation', 'monitor'])
def test_watch_cli_retries_rejected_media_without_advancing_point(collector, capsys, monkeypatch, failure):
    from chatstore.adapters import media_watch
    from chatstore.adapters.media_capture import MediaUnavailable
    from chatstore.adapters.monitor import MonitoringUnavailable

    dd, root, writer = collector
    filename = root / 'Attachments' / 'retry.bin'
    filename.write_bytes(b'bytes retained after retry')
    writer.execute("INSERT INTO attachment(ROWID,guid,filename,total_bytes) VALUES(1,'retry-guid',?,25)", (str(filename),))
    writer.execute('INSERT INTO message_attachment_join(message_id,attachment_id) VALUES(1,1)')
    writer.commit()
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    original = media_watch.prepare_media
    calls = 0
    def fail_after_retention(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        if calls == 1:
            error = MediaUnavailable if failure == 'mutation' else MonitoringUnavailable
            raise error('injected rejected media capture')
    monkeypatch.setattr(media_watch, 'prepare_media', fail_after_retention)
    sleeps = []
    def between_polls(seconds):
        sleeps.append(seconds)
        mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
        with NativeMirror(mirror_path) as mirror, closing(Cache(dd.cache)) as cache:
            assert mirror.pending() is None
            assert mirror.continuation() is None
            assert cache.get_checkpoint('messages', load_identity(dd).scope_for('messages'), 'native_capture') is None
    monkeypatch.setattr('chatstore.cli.app.time.sleep', between_polls)
    code = main(['--json', 'sync', '--source', 'messages', '--watch-interval', '900', '--max-polls', '2'])
    results = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert code == 0 and calls == 2 and len(sleeps) == 1
    assert len(results) == 2
    assert not results[0]['ok'] and results[0]['errors'][0]['code'] == 'capture_retry_required'
    assert results[1]['ok'] and results[1]['data'][0]['status'] == 'complete'
    with closing(Cache(dd.cache)) as cache:
        assert next(cache.iter_records('attachments'))['availability'] == 'available'


def test_watch_cli_reports_failure_when_media_remains_unavailable(collector, capsys, monkeypatch):
    from chatstore.adapters import media_watch
    from chatstore.adapters.monitor import MonitoringUnavailable

    dd, root, _ = collector
    cli(capsys, 'init', '--messages-path', str(root), '--timezone', 'UTC')
    monitors = []
    original = media_watch.SourceMonitor
    def track_monitor():
        monitor = original()
        monitors.append(monitor)
        return monitor
    def unavailable(*args, **kwargs):
        raise MonitoringUnavailable('descriptor coverage unavailable')
    monkeypatch.setattr(media_watch, 'SourceMonitor', track_monitor)
    monkeypatch.setattr(media_watch, 'prepare_media', unavailable)
    monkeypatch.setattr('chatstore.cli.app.time.sleep', lambda seconds: None)
    code = main(['--json', 'sync', '--source', 'messages', '--watch-interval', '900', '--max-polls', '2'])
    results = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert code == 1 and len(results) == 2
    assert all(not result['ok'] and result['data'] is None for result in results)
    assert all(result['errors'][0]['code'] == 'capture_retry_required' for result in results)
    assert len(monitors) == 2 and all(monitor._closed for monitor in monitors)
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation() is None
