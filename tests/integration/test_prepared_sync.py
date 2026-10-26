import sqlite3
from contextlib import closing
from dataclasses import replace

import pytest

from chatstore.adapters.native_mirror import MirrorUnavailable, NativeMirror
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, policy_signature, run_prepared_sync
from tests.fixtures.native_adapter import MirrorAdapter
from tests.unit.test_native_mirror import view


@pytest.fixture
def capture(tmp_path):
    path = tmp_path / 'native.sqlite'
    writer = sqlite3.connect(path)
    writer.execute('PRAGMA journal_mode=WAL')
    writer.execute('PRAGMA wal_autocheckpoint=0')
    writer.execute('CREATE TABLE message(id INTEGER PRIMARY KEY,body TEXT)')
    writer.execute("INSERT INTO message VALUES(1,'retained native body')")
    writer.commit()
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cfg, ident = Config(), Identity()
    scope, _ = ident.ensure_scope('messages')
    adapter = MirrorAdapter()
    with NativeMirror(tmp_path / 'mirror.sqlite') as mirror, closing(Cache(dd.cache)) as cache:
        with view(path) as source:
            pending = mirror.initialize(source, ['message'], namespace=f'messages/{scope}',
                policy_tag=policy_signature(adapter, cfg), observed_at='2026-10-18T10:00:00Z', context={})
        yield adapter, dd, cfg, ident, cache, PreparedSync(mirror, pending, 'chat'), writer, path
    writer.close()


def sync(capture):
    adapter, dd, cfg, ident, cache, prepared, *_ = capture
    return run_prepared_sync(adapter, dd, cfg, ident, cache, prepared)


def test_expired_native_source_captures_original_time_and_commits_receipt_with_checkpoint(capture, tmp_path):
    adapter, _, _, ident, cache, prepared, writer, path = capture
    writer.execute('DELETE FROM message')
    writer.commit()
    writer.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    path.rename(tmp_path / 'offline.sqlite')
    outcome = sync(capture)
    assert outcome.exit_code == 0 and adapter.calls == 1
    assert outcome.run['started_at'] == 1792317600000
    assert prepared.mirror.pending() is None
    assert prepared.mirror.continuation() == prepared.capture
    scope = ident.scope_for('messages')
    checkpoint = cache.get_checkpoint('messages', scope, 'native_capture')
    assert checkpoint['binding'] == prepared.binding
    assert checkpoint['capture']['token'] == prepared.capture.token
    saved = next(cache.iter_records('messages'))
    assert saved['text'] == 'retained native body' and saved['observed_at'] == outcome.run['started_at']
    assert cache.conn.execute('SELECT count(*) FROM native_capture_receipts').fetchone()[0] == 1
    assert prepared.mirror.path.exists()


def test_commit_before_ack_failure_retries_without_extraction_or_journal_changes(capture, monkeypatch):
    adapter, _, _, _, cache, prepared, *_ = capture
    acknowledge = prepared.mirror.acknowledge
    monkeypatch.setattr(prepared.mirror, 'acknowledge', lambda *args: (_ for _ in ()).throw(RuntimeError('interrupted ack')))
    with pytest.raises(RuntimeError, match='interrupted ack'):
        sync(capture)
    assert prepared.mirror.pending() == prepared.capture
    assert cache.conn.execute('SELECT count(*) FROM native_capture_receipts').fetchone()[0] == 1
    count = cache.conn.execute('SELECT count(*) FROM changes').fetchone()[0]
    monkeypatch.setattr(prepared.mirror, 'acknowledge', acknowledge)
    # Receipt recovery must not require the old decoder policy to remain installed.
    monkeypatch.setattr('chatstore.prepared_sync.policy_signature', lambda *args: 'upgraded-policy')
    assert sync(capture).exit_code == 0
    assert adapter.calls == 1
    assert cache.conn.execute('SELECT count(*) FROM changes').fetchone()[0] == count
    assert sync(capture).exit_code == 0 and adapter.calls == 1


def test_failed_or_partial_capture_has_no_receipt_or_native_checkpoint(capture):
    adapter, _, _, ident, cache, prepared, *_ = capture
    scope = ident.scope_for('messages')
    adapter.fail = True
    with pytest.raises(RuntimeError, match='extractor interruption'):
        sync(capture)
    assert prepared.mirror.pending() == prepared.capture
    assert not cache.conn.execute('SELECT 1 FROM native_capture_receipts').fetchone()
    assert cache.get_checkpoint('messages', scope, 'native_capture') is None
    adapter.fail = False
    adapter.partial = True
    assert sync(capture).exit_code == 5
    assert prepared.mirror.pending() == prepared.capture
    assert not cache.conn.execute('SELECT 1 FROM native_capture_receipts').fetchone()
    adapter.partial = False
    assert sync(capture).exit_code == 0
    assert cache.conn.execute('SELECT count(*) FROM records WHERE kind=\'sync_runs\'').fetchone()[0] == 1


def test_receipt_and_sync_completion_roll_back_together(capture, monkeypatch):
    _, _, _, ident, cache, prepared, *_ = capture
    commit = PreparedSync.commit_receipt
    def fail_after_receipt(self, *args):
        commit(self, *args)
        raise RuntimeError('crash before canonical commit')
    monkeypatch.setattr(PreparedSync, 'commit_receipt', fail_after_receipt)
    with pytest.raises(RuntimeError, match='before canonical commit'):
        sync(capture)
    assert prepared.mirror.pending() == prepared.capture
    assert not cache.conn.execute('SELECT 1 FROM native_capture_receipts').fetchone()
    assert cache.get(prepared.run_urn)['status'] == 'running'
    scope = ident.scope_for('messages')
    assert cache.get_checkpoint('messages', scope, 'adapter') is None
    assert cache.get_checkpoint('messages', scope, 'native_capture') is None
    monkeypatch.setattr(PreparedSync, 'commit_receipt', commit)
    assert sync(capture).exit_code == 0


def test_receipt_can_be_rebuilt_from_restored_canonical_run(capture):
    adapter, _, _, ident, cache, prepared, *_ = capture
    assert sync(capture).exit_code == 0
    with cache.write():
        cache.conn.execute('DELETE FROM native_capture_receipts')
        cache.conn.execute("DELETE FROM checkpoints WHERE key='native_capture'")
    assert prepared.receipt(cache, adapter.source, ident.scope_for(adapter.source))
    assert sync(capture).exit_code == 0 and adapter.calls == 1


def test_wrong_capture_policy_scope_or_file_binding_is_rejected(capture):
    adapter, dd, cfg, ident, cache, prepared, *_ = capture
    for change in ({'policy_tag': 'wrong'}, {'namespace': 'wrong'}, {'observed_at': '2026-10-18T10:01:00Z'}):
        bad = replace(prepared, capture=replace(prepared.capture, **change))
        with pytest.raises(MirrorUnavailable, match='exact pending'):
            run_prepared_sync(adapter, dd, cfg, ident, cache, bad)
    assert sync(capture).exit_code == 0
    bad = replace(prepared, file_name='different')
    with pytest.raises(MirrorUnavailable, match='binding conflict'):
        run_prepared_sync(adapter, dd, cfg, ident, cache, bad)


def test_encrypted_cold_restore_rebuilds_capture_receipt_without_original_cache_or_source(capture, tmp_path):
    from pathlib import Path

    from chatstore.archive.import_ import import_archive
    from chatstore.archive.incremental import export_changes

    adapter, dd, cfg, ident, cache, prepared, _, path = capture
    assert sync(capture).exit_code == 0
    archive = export_changes(cache, dd, cfg, ident, consumer='cold-restore', output=dd.exports, password='test-password')
    scope = ident.scope_for(adapter.source)
    original_receipt = prepared.receipt(cache, adapter.source, scope)
    cache.close()
    cache.path.rename(tmp_path / 'offline-cache.sqlite')
    path.rename(tmp_path / 'offline-source.sqlite')
    target = DataDir(tmp_path / 'restored')
    target.create()
    with closing(Cache(target.cache)) as restored:
        result = import_archive(restored, target, Identity(), Path(archive['path']), 'test-password')
        assert result.action == 'imported' and result.conflicts == 0
        assert not restored.conn.execute('SELECT 1 FROM native_capture_receipts').fetchone()
        assert prepared.receipt(restored, adapter.source, scope) == original_receipt
        assert next(restored.iter_records('messages'))['text'] == 'retained native body'
        assert restored.get_checkpoint(adapter.source, scope, 'native_capture')['binding'] == prepared.binding


def test_child_capture_requires_canonical_parent_before_advancing(capture):
    from chatstore.adapters.wal import WalPages

    adapter, dd, cfg, ident, cache, prepared, writer, path = capture
    # A mirror acknowledgement alone is not a canonical receipt.
    prepared.mirror.acknowledge(prepared.capture.token, 'not-a-canonical-receipt')
    writer.execute("UPDATE message SET body='child source body'")
    writer.commit()
    with view(path, prepared.capture.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        child = prepared.mirror.prepare(source, pages, expected_token=prepared.capture.token,
            namespace=prepared.capture.namespace, policy_tag=prepared.capture.policy_tag,
            observed_at='2026-10-18T10:15:00Z', context={})
    with pytest.raises(MirrorUnavailable, match='parent has no complete'):
        run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(prepared.mirror, child, 'chat'))
    assert prepared.mirror.pending() == child
    assert not cache.conn.execute('SELECT 1 FROM native_capture_receipts').fetchone()


def test_acknowledged_parent_then_child_captures_retained_edit_and_absence(capture):
    from chatstore.adapters.wal import WalPages

    adapter, dd, cfg, ident, cache, prepared, writer, path = capture
    assert sync(capture).exit_code == 0
    writer.execute("UPDATE message SET body='historical source edit'")
    writer.commit()
    with view(path, prepared.capture.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        child = prepared.mirror.prepare(source, pages, expected_token=prepared.capture.token,
            namespace=prepared.capture.namespace, policy_tag=prepared.capture.policy_tag,
            observed_at='2026-10-18T10:15:00Z', context={})
    next_sync = PreparedSync(prepared.mirror, child, 'chat')
    assert run_prepared_sync(adapter, dd, cfg, ident, cache, next_sync).exit_code == 0
    saved = next(cache.iter_records('messages'))
    assert saved['text'] == 'historical source edit'
    assert {cache.revision_record(saved['urn'], row['revision_digest'])['text'] for row in cache.revisions_of(saved['urn'])} == {'retained native body', 'historical source edit'}
    writer.execute('DELETE FROM message')
    writer.commit()
    with view(path, child.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        deletion = prepared.mirror.prepare(source, pages, expected_token=child.token,
            namespace=child.namespace, policy_tag=child.policy_tag, observed_at='2026-10-18T10:30:00Z', context={})
    assert run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(prepared.mirror, deletion, 'chat')).exit_code == 0
    assert cache.observations_of(saved['urn'])[0]['present'] == 'absent_from_source'
    assert cache.conn.execute('SELECT count(*) FROM native_capture_receipts').fetchone()[0] == 3


def test_competing_processes_extract_prepared_point_only_once(capture, tmp_path):
    import json
    import subprocess
    import sys

    adapter, dd, cfg, ident, cache, prepared, *_ = capture
    inputs = tmp_path / 'inputs.json'
    inputs.write_text(json.dumps({'config': cfg.to_json(), 'identity': ident.to_json()}))
    marker = tmp_path / 'extraction-calls'
    script = """
import json
import os
import sys
import time
from pathlib import Path
from tests.fixtures.native_adapter import MirrorAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, run_prepared_sync
mirror_path, data_root, inputs, marker = map(Path, sys.argv[1:])
settings = json.loads(inputs.read_text())
adapter = MirrorAdapter()
extract = adapter.extract
def counted(*args, **kwargs):
    fd = os.open(marker, os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    os.write(fd, b'extract\\n')
    os.close(fd)
    time.sleep(0.1)
    yield from extract(*args, **kwargs)
adapter.extract = counted
mirror = NativeMirror(mirror_path)
prepared = PreparedSync(mirror, mirror.pending() or mirror.continuation(), 'chat')
dd = DataDir(data_root)
cache = Cache(dd.cache)
outcome = run_prepared_sync(adapter, dd, Config.from_json(settings['config']), Identity.from_json(settings['identity']), cache, prepared)
assert outcome.exit_code == 0
cache.close()
mirror.close()
"""
    command = [sys.executable, '-c', script, str(prepared.mirror.path), str(dd.root), str(inputs), str(marker)]
    processes = [subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) for _ in range(2)]
    try:
        for process in processes:
            _, errors = process.communicate(timeout=20)
            assert process.returncode == 0, errors
    finally:
        for process in processes:
            if process.poll() is None:
                process.kill()
                process.wait()
    assert marker.read_text().splitlines() == ['extract']
    assert cache.conn.execute('SELECT count(*) FROM native_capture_receipts').fetchone()[0] == 1
    assert prepared.mirror.pending() is None
    assert adapter.calls == 0


def test_messages_adapter_captures_offline_text_mirror_like_ordinary_sync(tmp_path):
    from chatstore.adapters.messages.adapter import MessagesAdapter, find_helper
    from chatstore.sync import run_sync
    from tests.fixtures.synthetic import build_messages

    adapter = MessagesAdapter()
    cfg, ident = Config(), Identity()
    if find_helper(cfg) is None:
        pytest.skip('Rust Messages helper is unavailable')
    root = build_messages(tmp_path / 'messages')
    cfg.source_paths['messages'] = str(root)
    scope, _ = ident.ensure_scope('messages')
    path = root / 'chat.db'
    with closing(sqlite3.connect(path)) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        # This test isolates row capture; durable media preparation is separate.
        writer.execute('DELETE FROM message_attachment_join')
        writer.execute('DELETE FROM attachment')
        writer.commit()
        ordinary = DataDir(tmp_path / 'ordinary')
        ordinary.create()
        with closing(Cache(ordinary.cache)) as expected:
            assert run_sync(adapter, ordinary, cfg, ident, expected).exit_code == 0
            expected_heads = dict(expected.conn.execute("SELECT urn,revision_digest FROM records WHERE kind!='sync_runs'"))
        target = DataDir(tmp_path / 'prepared')
        target.create()
        with NativeMirror(tmp_path / 'messages-mirror.sqlite') as mirror, closing(Cache(target.cache)) as cache:
            with view(path) as source:
                tables = [row[0] for row in source.connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
                pending = mirror.initialize(source, tables, namespace=f'messages/{scope}',
                    policy_tag=policy_signature(adapter, cfg), observed_at='2026-10-18T10:00:00Z', context={})
            root.rename(tmp_path / 'offline-messages')
            outcome = run_prepared_sync(adapter, target, cfg, ident, cache, PreparedSync(mirror, pending, 'chat'))
            assert outcome.exit_code == 0, outcome.run
            actual_heads = dict(cache.conn.execute("SELECT urn,revision_digest FROM records WHERE kind!='sync_runs'"))
            assert actual_heads == expected_heads
            assert mirror.pending() is None



def test_prepared_observation_timestamp_preserves_milliseconds_and_timezone(capture):
    prepared = capture[5]
    milliseconds = replace(prepared, capture=replace(prepared.capture, observed_at='2026-10-18T10:00:00.123Z'))
    offset = replace(prepared, capture=replace(prepared.capture, observed_at='2026-10-18T12:00:00.123+02:00'))
    assert milliseconds.observed_at_ms == prepared.observed_at_ms + 123
    assert offset.observed_at_ms == milliseconds.observed_at_ms
    naive = replace(prepared, capture=replace(prepared.capture, observed_at='2026-10-18T10:00:00'))
    with pytest.raises(MirrorUnavailable, match='timezone'):
        _ = naive.observed_at_ms


@pytest.mark.parametrize('target', [
    'monitor.SourceMonitor.unchanged', 'native_mirror._same_row', 'page_index.PageIndex.build',
    'pages.decode_tree_page', 'source_view._read_index', 'wal._checksum',
    'wal_image.WalPageImage.tree_page',
])
def test_raw_projection_implementation_change_invalidates_pending_capture(capture, monkeypatch, target):
    from chatstore import prepared_sync

    adapter, dd, cfg, ident, cache, prepared, *_ = capture
    parts = target.split('.')
    owner = getattr(prepared_sync, parts[0])
    for part in parts[1:-1]:
        owner = getattr(owner, part)
    original = getattr(owner, parts[-1])
    def changed(*args, **kwargs):
        raise AssertionError('incompatible projection must not run')
    monkeypatch.setattr(owner, parts[-1], changed)
    assert policy_signature(adapter, cfg) != prepared.capture.policy_tag
    with pytest.raises(MirrorUnavailable, match='policy mismatch'):
        run_prepared_sync(adapter, dd, cfg, ident, cache, prepared)
    assert adapter.calls == 0
    assert prepared.mirror.pending() == prepared.capture
    monkeypatch.setattr(owner, parts[-1], original)
    assert sync(capture).exit_code == 0
