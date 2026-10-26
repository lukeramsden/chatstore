import json
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.wal import WalPages
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, policy_signature, run_prepared_sync
from tests.fixtures.synthetic import build_messages
from tests.unit.test_native_mirror import view


@pytest.fixture
def history(tmp_path):
    helper = Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        pytest.skip('build the development Messages helper first')
    root = build_messages(tmp_path / 'messages', extra_messages=100)
    path = root / 'chat.db'
    adapter = MessagesAdapter()
    cfg = Config(source_paths={'messages': str(root)}, helper_path=str(helper))
    ident = Identity()
    scope, _ = ident.ensure_scope('messages')
    dd = DataDir(tmp_path / 'data')
    dd.create()
    with closing(sqlite3.connect(path)) as writer, NativeMirror(tmp_path / 'mirror.sqlite') as mirror, closing(Cache(dd.cache)) as cache:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('DELETE FROM message_attachment_join')
        writer.execute('DELETE FROM attachment')
        writer.commit()
        with view(path) as source:
            tables = [row[0] for row in source.connection.execute("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
            pending = mirror.initialize(source, tables, namespace=f'messages/{scope}', policy_tag=policy_signature(adapter, cfg), observed_at='2026-10-18T10:00:00Z', context={})
        prepared = PreparedSync(mirror, pending, 'chat')
        initial = run_prepared_sync(adapter, dd, cfg, ident, cache, prepared)
        assert initial.exit_code == 0, initial.run
        yield adapter, dd, cfg, ident, cache, mirror, writer, path, pending


def prepare(history, parent=None, step=1):
    _, _, _, _, _, mirror, _, path, initial = history
    parent = initial if parent is None else parent
    with view(path, parent.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        return mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at=f'2026-10-18T10:{step * 5:02d}:00Z', context={})


def sync(history, capture):
    adapter, dd, cfg, ident, cache, mirror, *_ = history
    return run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, capture, 'chat'))


def test_old_edit_clean_and_delete_preserve_untouched_freshness_and_history(history):
    _, _, _, _, cache, _, writer, _, _ = history
    original = next(value for value in cache.iter_records('messages') if value['text'] == 'hello dave')
    kept = next(value for value in cache.iter_records('messages') if value['text'] == 'extra message 50')
    writer.execute("UPDATE message SET text='selectively captured old edit' WHERE ROWID=1")
    writer.commit()
    edited = prepare(history)
    result = sync(history, edited)
    assert result.exit_code == 0, result.run
    assert result.run['counts']['messages'] == 1
    assert result.run['counts']['canonical_units_rebuilt'] == 1
    assert cache.get(original['urn'])['text'] == 'selectively captured old edit'
    assert cache.get(kept['urn'])['sync_run'] == PreparedSync(history[5], edited, 'chat').run_urn
    assert cache.observations_of(kept['urn'])[0]['observed_at'] == result.run['started_at']
    clean = prepare(history, edited, 2)
    result = sync(history, clean)
    assert result.exit_code == 0 and result.run['counts'].get('messages', 0) == 0
    writer.execute('DELETE FROM message WHERE ROWID=1')
    writer.commit()
    deletion = prepare(history, clean, 3)
    result = sync(history, deletion)
    assert result.exit_code == 0 and result.run['counts'].get('messages', 0) == 0
    assert cache.get(original['urn'])['text'] == 'selectively captured old edit'
    assert cache.observations_of(original['urn'])[0]['present'] == 'absent_from_source'
    assert cache.get(original['urn'])['sync_run'] != PreparedSync(history[5], deletion, 'chat').run_urn
    assert cache.get(kept['urn'])['sync_run'] == PreparedSync(history[5], deletion, 'chat').run_urn
    assert len(cache.revisions_of(original['urn'])) == 2
    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == deletion.token


def test_header_and_stub_changes_reconcile_only_affected_context(history):
    _, _, _, _, cache, _, writer, _, _ = history
    untouched_stub = next(value for value in cache.iter_records('chats') if value['assignment'] == 'stub')
    writer.execute("UPDATE chat SET display_name='changed header title' WHERE ROWID=1")
    writer.commit()
    changed = prepare(history)
    result = sync(history, changed)
    assert result.exit_code == 0 and result.run['counts'].get('messages', 0) == 0
    assert cache.observations_of(untouched_stub['urn'])[0]['present'] == 'present'
    assert cache.get(untouched_stub['urn'])['sync_run'] == PreparedSync(history[5], changed, 'chat').run_urn
    writer.execute('INSERT INTO chat_message_join(chat_id,message_id) VALUES(1,6)')
    writer.commit()
    joined = prepare(history, changed, 2)
    result = sync(history, joined)
    assert result.exit_code == 0 and result.run['counts']['messages'] == 1
    assert cache.observations_of(untouched_stub['urn'])[0]['present'] == 'absent_from_source'
    assert cache.get(untouched_stub['urn'])['sync_run'] != PreparedSync(history[5], joined, 'chat').run_urn


def test_receipt_failure_rolls_back_final_coverage_and_resumes_partial_epoch(history, monkeypatch):
    _, _, _, _, cache, mirror, writer, _, initial = history
    writer.execute("UPDATE message SET text='survives partial canonical attempt' WHERE ROWID=1")
    writer.commit()
    pending = prepare(history)
    commit = PreparedSync.commit_receipt
    def fail(self, *args):
        commit(self, *args)
        raise RuntimeError('final receipt interruption')
    monkeypatch.setattr(PreparedSync, 'commit_receipt', fail)
    with pytest.raises(RuntimeError, match='receipt interruption'):
        sync(history, pending)
    assert mirror.pending() == pending
    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == initial.token
    row = cache.conn.execute('SELECT token,staged_records FROM capture_coverage_attempts').fetchone()
    assert row[0] == pending.token and json.loads(row[1])
    monkeypatch.setattr(PreparedSync, 'commit_receipt', commit)
    result = sync(history, pending)
    assert result.exit_code == 0 and result.run['counts']['messages'] == 1
    assert mirror.pending() is None
    assert not cache.conn.execute('SELECT 1 FROM capture_coverage_attempts').fetchone()


def test_unrelated_canonical_edit_forces_full_mirror_recovery(history):
    from chatstore.canonical.records import restamp

    _, _, _, _, cache, _, writer, _, _ = history
    kept = next(value for value in cache.iter_records('messages') if value['text'] == 'extra message 50')
    with cache.write():
        cache.upsert(restamp(dict(kept, text='unrelated canonical writer')), observed_at=1792317610000)
    writer.execute("UPDATE message SET text='next source change' WHERE ROWID=1")
    writer.commit()
    pending = prepare(history)
    result = sync(history, pending)
    assert result.exit_code == 0 and result.run['mode'] == 'full_reconcile'
    assert result.run['counts']['messages'] == 107
    assert result.run['counts']['canonical_units_reused'] > 100
    assert cache.get(kept['urn'])['text'] == 'extra message 50'


def test_abrupt_exit_after_removed_child_batch_resumes_original_withdrawal(history, tmp_path):
    import subprocess
    import sys

    _, dd, cfg, ident, cache, mirror, writer, path, _ = history
    writer.execute("INSERT INTO attachment(ROWID,guid,uti,mime_type,transfer_name,total_bytes,is_sticker) VALUES(2,'cloud-attachment','public.png','image/png','cloud.png',12,0)")
    writer.execute('INSERT INTO message_attachment_join VALUES(3,2)')
    writer.commit()
    attached = prepare(history)
    assert sync(history, attached).exit_code == 0
    attachment = next(cache.iter_records('attachments'))
    assert attachment['availability'] == 'not_downloaded'
    writer.execute('DELETE FROM message_attachment_join WHERE attachment_id=2')
    writer.execute('DELETE FROM attachment WHERE ROWID=2')
    writer.commit()
    removed = prepare(history, attached, 2)
    inputs = tmp_path / 'inputs.json'
    inputs.write_text(json.dumps({'config': cfg.to_json(), 'identity': ident.to_json()}))
    program = """
import json
import os
import sys
from pathlib import Path
from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.cache import Cache
from chatstore.cache.capture_coverage import CaptureCoverage
from chatstore.config import Config, Identity
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync, run_prepared_sync
mirror_path, data_root, inputs = map(Path, sys.argv[1:])
settings = json.loads(inputs.read_text())
mirror = NativeMirror(mirror_path)
cache = Cache(DataDir(data_root).cache)
CaptureCoverage.finish = lambda *args, **kwargs: os._exit(23)
run_prepared_sync(MessagesAdapter(), DataDir(data_root), Config.from_json(settings['config']), Identity.from_json(settings['identity']), cache, PreparedSync(mirror, mirror.pending(), 'chat'))
"""
    result = subprocess.run([sys.executable, '-c', program, str(mirror.path), str(dd.root), str(inputs)], capture_output=True, text=True, check=False, timeout=20)
    assert result.returncode == 23, result.stderr
    assert mirror.pending() == removed
    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == attached.token
    assert cache.observations_of(attachment['urn'])[0]['present'] == 'present'
    outcome = sync(history, removed)
    assert outcome.exit_code == 0
    assert outcome.run['counts']['messages'] == 1
    assert cache.observations_of(attachment['urn'])[0]['present'] == 'absent_from_source'
    assert cache.get(attachment['urn'])['availability'] == 'not_downloaded'
    assert mirror.pending() is None
    assert path.exists()


def test_file_backed_attachment_cannot_authorize_sql_only_selective_sightings(history):
    _, _, _, _, cache, _, writer, _, _ = history
    writer.execute("INSERT INTO attachment(ROWID,guid,filename,uti,mime_type,transfer_name,total_bytes,is_sticker) VALUES(2,'file-backed',?,'public.png','image/png','file.png',12,0)", (str(history[7].parent / 'missing-file.png'),))
    writer.execute('INSERT INTO message_attachment_join VALUES(3,2)')
    writer.commit()
    pending = prepare(history)
    result = sync(history, pending)
    assert result.exit_code == 0 and result.run['mode'] == 'full_reconcile'
    assert result.run['counts']['messages'] == 107
    assert next(cache.iter_records('attachments'))['availability'] == 'missing'



def test_explicit_full_capture_rebuilds_units_while_coverage_recovery_can_reuse_them(history):
    adapter, dd, cfg, ident, cache, mirror, *_ = history
    pending = prepare(history)
    outcome = run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, pending, 'chat'), full=True)
    assert outcome.exit_code == 0 and outcome.run['mode'] == 'full_reconcile'
    assert outcome.run['counts']['canonical_units_rebuilt'] == 107
    assert outcome.run['counts'].get('canonical_units_reused', 0) == 0
    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == pending.token


@pytest.mark.parametrize('reason', ['schema', 'policy', 'wal-reset'])
def test_rebase_reconciles_old_absence_then_resumes_selective_capture(history, reason):
    adapter, _, cfg, _, cache, mirror, writer, path, initial = history
    original = next(value for value in cache.iter_records('messages') if value['text'] == 'hello dave')
    edited = next(value for value in cache.iter_records('messages') if value['text'] == 'extra message 50')
    if reason == 'schema':
        writer.execute('ALTER TABLE message ADD COLUMN rebase_context TEXT')
    elif reason == 'policy':
        cfg.timezone = 'Europe/London'
        assert policy_signature(adapter, cfg) != initial.policy_tag
    else:
        assert writer.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0] == 0
    writer.execute('DELETE FROM message WHERE ROWID=1')
    writer.execute("UPDATE message SET text='changed across schema rebase' WHERE ROWID=150")
    writer.commit()
    with view(path) as source:
        tables = [row['name'] for row in mirror.conn.execute('SELECT name FROM cs_native_mirror_tables')]
        rebased = mirror.rebase(source, tables, expected_token=initial.token,
            namespace=initial.namespace, policy_tag=policy_signature(adapter, cfg),
            observed_at='2026-10-18T10:15:00Z', context={'reason': reason})
    result = sync(history, rebased)
    assert result.exit_code == 0, result.run
    assert cache.observations_of(original['urn'])[0]['present'] == 'absent_from_source'
    assert cache.get(original['urn'])['text'] == original['text']
    assert cache.get(edited['urn'])['text'] == 'changed across schema rebase'
    assert len(cache.revisions_of(edited['urn'])) == 2
    assert cache.conn.execute('SELECT token FROM capture_coverage_points').fetchone()[0] == rebased.token
    writer.execute("UPDATE message SET text='selective after rebase' WHERE ROWID=150")
    writer.commit()
    child = prepare(history, rebased, 4)
    result = sync(history, child)
    assert result.exit_code == 0, result.run
    assert result.run['counts']['messages'] == 1
    assert cache.get(edited['urn'])['text'] == 'selective after rebase'
    assert len(cache.revisions_of(edited['urn'])) == 3
