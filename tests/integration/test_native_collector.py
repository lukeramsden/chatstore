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
