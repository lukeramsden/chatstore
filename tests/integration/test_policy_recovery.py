import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from chatstore import prepared_sync
from chatstore.adapters.messages.adapter import MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir
from chatstore.prepared_sync import PreparedSync
from tests.fixtures.synthetic import build_messages, build_whatsapp

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='native collector policy recovery')


@pytest.fixture(params=['messages', 'whatsapp'])
def source(request, tmp_path):
    name = request.param
    root = (build_messages if name == 'messages' else build_whatsapp)(tmp_path / name)
    helper = Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if name == 'messages' and not helper.exists():
        pytest.skip('build development Messages helper first')
    cfg = Config(source_paths={name: str(root)}, helper_path=str(helper) if name == 'messages' else None)
    ident = Identity()
    ident.ensure_scope(name)
    dd = DataDir(tmp_path / 'data')
    dd.create()
    filename = 'chat.db' if name == 'messages' else 'ChatStorage.sqlite'
    adapter = MessagesAdapter() if name == 'messages' else WhatsAppAdapter()
    with closing(sqlite3.connect(root / filename)) as writer, closing(Cache(dd.cache)) as cache:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('UPDATE attachment SET filename=NULL' if name == 'messages' else 'UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH=NULL')
        writer.commit()
        yield name, root, filename, adapter, cfg, ident, dd, cache, writer


def pending_after_interruption(source, monkeypatch):
    _, _, _, adapter, cfg, ident, dd, cache, _ = source
    def interrupted(*args, **kwargs):
        raise RuntimeError('interrupt before canonical receipt')
    with monkeypatch.context() as patch:
        patch.setattr(PreparedSync, 'commit_receipt', interrupted)
        with pytest.raises(RuntimeError, match='interrupt before canonical'):
            collect_native(adapter, dd, cfg, ident, cache)
    path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(path) as mirror:
        pending = mirror.pending()
        assert pending is not None
    assert cache.conn.execute('SELECT count(*) FROM capture_coverage_attempts').fetchone()[0] == 1
    assert cache.conn.execute('SELECT count(*) FROM native_capture_receipts').fetchone()[0] == 0
    return path, pending


@pytest.mark.parametrize('interrupt_recovery', [False, True])
def test_policy_upgrade_recovers_staged_capture_offline_and_preserves_original_provenance(source, monkeypatch, interrupt_recovery):
    name, root, filename, adapter, cfg, ident, dd, cache, _ = source
    mirror_path, original = pending_after_interruption(source, monkeypatch)
    prior = {row['urn']: row['revision_digest'] for row in cache.iter_records('messages')}
    with NativeMirror(mirror_path) as mirror:
        old_run = PreparedSync(mirror, original, 'chat' if name == 'messages' else 'ChatStorage').run_urn
    (root / filename).rename(root / ('offline-' + filename))
    if name == 'whatsapp':
        (root / 'LID.sqlite').rename(root / 'offline-LID.sqlite')
    current_policy = ['upgraded-policy']
    monkeypatch.setattr(prepared_sync, 'policy_signature', lambda *args: current_policy[0])
    if interrupt_recovery:
        def interrupted(*args, **kwargs):
            raise RuntimeError('interrupt after native policy recovery')
        with monkeypatch.context() as patch:
            patch.setattr(prepared_sync, '_retire_recovered_attempts', interrupted)
            with pytest.raises(RuntimeError, match='after native policy recovery'):
                collect_native(adapter, dd, cfg, ident, cache)
        with NativeMirror(mirror_path) as mirror:
            assert mirror.pending().token != original.token
        assert cache.conn.execute('SELECT count(*) FROM capture_coverage_attempts').fetchone()[0] == 1
        current_policy[0] = 'upgraded-policy-again'
    outcome = collect_native(adapter, dd, cfg, ident, cache)
    assert outcome is not None and outcome.exit_code == 0, outcome
    capture = outcome.run['snapshot']['prepared_native']['capture']
    assert capture['token'] != original.token and capture['policy_tag'] == current_policy[0]
    assert capture['observed_at'] == original.observed_at
    proof = capture['context']['policy_recovery']
    tokens = []
    while proof is not None:
        tokens.append(proof['capture']['token'])
        proof = proof['capture']['context'].get('policy_recovery')
    assert original.token in tokens
    assert capture['context']['native_rebase_required']
    assert {row['urn']: row['revision_digest'] for row in cache.iter_records('messages')} == prior
    assert cache.get(old_run)['status'] == 'failed'
    assert any(error['code'] == 'policy_recovered' for error in cache.get(old_run)['errors'])
    assert cache.conn.execute('SELECT count(*) FROM capture_coverage_attempts').fetchone()[0] == 0
    assert cache.get_checkpoint(name, ident.scope_for(name), 'native_capture')['capture']['token'] == capture['token']
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None
        assert mirror.continuation().token == capture['token']
        assert mirror.continuation().cursor == original.cursor
    from chatstore.archive.import_ import import_archive
    from chatstore.archive.incremental import export_changes
    exported = export_changes(cache, dd, cfg, ident, consumer='policy-restore', output=dd.exports,
                              password='test-password')
    assert not exported['has_more']
    target = DataDir(dd.root / 'restored')
    target.create()
    with closing(Cache(target.cache)) as restored:
        imported = import_archive(restored, target, Identity(), Path(exported['path']), 'test-password')
        assert imported.action == 'imported'
        assert {row['urn']: row['revision_digest'] for row in restored.iter_records('messages')} == prior
        assert restored.get(old_run)['status'] == 'failed'
        complete = next(row for row in restored.iter_records('sync_runs') if row['status'] == 'complete')
        assert complete['snapshot']['prepared_native']['capture'] == capture


def test_live_poll_after_policy_recovery_rebases_before_native_continuation(source, monkeypatch):
    from chatstore import native_collector, native_whatsapp

    name, _, _, adapter, cfg, ident, dd, cache, writer = source
    _, original = pending_after_interruption(source, monkeypatch)
    monkeypatch.setattr(prepared_sync, 'policy_signature', lambda *args: 'upgraded-policy')
    monkeypatch.setattr(native_collector, 'policy_signature', lambda *args: 'upgraded-policy')
    monkeypatch.setattr(native_whatsapp, 'policy_signature', lambda *args: 'upgraded-policy')
    recovered = collect_native(adapter, dd, cfg, ident, cache)
    assert recovered is not None and recovered.exit_code == 0
    token = recovered.run['snapshot']['prepared_native']['capture']['token']
    assert token != original.token
    writer.execute("UPDATE message SET text='post-recovery live change' WHERE ROWID=1" if name == 'messages'
                   else "UPDATE ZWAMESSAGE SET ZTEXT='post-recovery live change' WHERE Z_PK=1")
    writer.commit()
    next_run = collect_native(adapter, dd, cfg, ident, cache)
    assert next_run is not None and next_run.exit_code == 0
    capture = next_run.run['snapshot']['prepared_native']['capture']
    assert capture['baseline'] and capture['parent'] == token
    assert not capture['context'].get('native_rebase_required')
    assert any(row['text'] == 'post-recovery live change' for row in cache.iter_records('messages'))


def test_policy_recovery_refuses_missing_raw_schema_without_advancing(source, monkeypatch):
    from chatstore.adapters.messages import adapter as messages
    from chatstore.adapters.native_mirror import MirrorUnavailable
    from chatstore.adapters.whatsapp import adapter as whatsapp

    name, _, _, adapter, cfg, ident, dd, cache, _ = source
    path, original = pending_after_interruption(source, monkeypatch)
    module = messages if name == 'messages' else whatsapp
    table = 'message' if name == 'messages' else 'ZWAMESSAGE'
    monkeypatch.setitem(module.REQUIRED, table, module.REQUIRED[table] | {'required_by_new_policy'})
    assert prepared_sync.policy_signature(adapter, cfg) != original.policy_tag
    with pytest.raises(MirrorUnavailable, match='schema migration'):
        collect_native(adapter, dd, cfg, ident, cache)
    with NativeMirror(path) as mirror:
        assert mirror.pending() == original
    assert cache.conn.execute('SELECT count(*) FROM capture_coverage_attempts').fetchone()[0] == 1
    assert cache.conn.execute('SELECT count(*) FROM native_capture_receipts').fetchone()[0] == 0


def test_corrupted_recovery_binding_cannot_retire_original_coverage(source, monkeypatch):
    import json

    from chatstore.adapters.native_mirror import MirrorUnavailable

    name, _, _, adapter, cfg, ident, dd, cache, _ = source
    path, original = pending_after_interruption(source, monkeypatch)
    monkeypatch.setattr(prepared_sync, 'policy_signature', lambda *args: 'upgraded-policy')
    def interrupted(*args, **kwargs):
        raise RuntimeError('interrupt recovered point before takeover')
    with monkeypatch.context() as patch:
        patch.setattr(prepared_sync, '_retire_recovered_attempts', interrupted)
        with pytest.raises(RuntimeError, match='before takeover'):
            collect_native(adapter, dd, cfg, ident, cache)
    with NativeMirror(path) as mirror:
        pending = mirror.pending()
        context = dict(pending.context)
        context['policy_recovery']['metadata']['binding'] = 'invalid'
        mirror.conn.execute('UPDATE cs_native_mirror_state SET context=? WHERE id=1', (json.dumps(context),))
    with pytest.raises(MirrorUnavailable, match='provenance binding mismatch'):
        collect_native(adapter, dd, cfg, ident, cache)
    assert cache.conn.execute('SELECT token FROM capture_coverage_attempts').fetchone()[0] == original.token
    assert cache.get_checkpoint(name, ident.scope_for(name), 'native_capture') is None
    with NativeMirror(path) as mirror:
        assert mirror.pending() is not None
