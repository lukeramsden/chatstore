import hashlib
import os
import sqlite3
import subprocess
import sys
import time
from contextlib import closing
from functools import partial
from pathlib import Path

import pytest

from chatstore.adapters.messages.adapter import REQUIRED, MessagesAdapter
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.prepared_media import prepare_media, read_media
from chatstore.adapters.wal import WalPages
from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.paths import DataDir, blob_path
from chatstore.prepared_sync import PreparedSync, policy_signature, run_prepared_sync
from tests.fixtures.synthetic import build_messages
from tests.unit.test_native_mirror import view


@pytest.fixture
def media_history(tmp_path):
    helper = Path(__file__).resolve().parents[2] / 'helpers/messages-decoder/target/release/chatstore-messages-decoder'
    if not helper.is_file():
        pytest.skip('build development Messages helper first')
    root = build_messages(tmp_path / 'messages', extra_messages=20)
    path = root / 'chat.db'
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cfg = Config(source_paths={'messages': str(root)}, helper_path=str(helper))
    ident = Identity()
    scope, _ = ident.ensure_scope('messages')
    adapter = MessagesAdapter()
    with closing(sqlite3.connect(path)) as writer, NativeMirror(tmp_path / 'mirror.sqlite') as mirror, closing(Cache(dd.cache)) as cache:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('PRAGMA user_version=1')
        writer.commit()
        filename = Path(writer.execute('SELECT filename FROM attachment WHERE ROWID=1').fetchone()[0])
        with view(path) as source:
            capture = mirror.initialize(source, [*REQUIRED, 'chat_recoverable_message_join'],
                namespace=f'messages/{scope}', policy_tag=policy_signature(adapter, cfg),
                observed_at='2026-10-18T10:00:00Z', context={'messages_media': {'format': 1}},
                validate_rows=partial(prepare_media, root=root, dd=dd, full=True))
        yield adapter, dd, cfg, ident, cache, mirror, writer, path, filename, capture


def sync(history, capture):
    adapter, dd, cfg, ident, cache, mirror, *_ = history
    return run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, capture, 'chat'))


def test_retained_media_captures_and_archives_after_native_files_expire(media_history, tmp_path):
    _, dd, cfg, ident, cache, mirror, _, path, filename, capture = media_history
    original = filename.read_bytes()
    digest = hashlib.sha256(original).hexdigest()
    filename.unlink()
    path.rename(tmp_path / 'offline-source.sqlite')
    outcome = sync(media_history, capture)
    assert outcome.exit_code == 0, outcome.run
    attachment = next(cache.iter_records('attachments'))
    assert attachment['availability'] == 'available' and attachment['blob_sha256'] == digest
    assert attachment['source_path_hint'] == 'Attachments/ab/01/IMG_0001.HEIC'
    assert blob_path(dd, digest).read_bytes() == original
    assert mirror.continuation() == capture
    archive = export_changes(cache, dd, cfg, ident, consumer='retained-media', output=dd.exports,
                             password='test-password', media='available-media')
    target = DataDir(tmp_path / 'cold-restore')
    target.create()
    dd.blobs.rename(tmp_path / 'offline-original-blobs')
    with closing(Cache(target.cache)) as restored:
        imported = import_archive(restored, target, Identity(), Path(archive['path']), 'test-password')
        assert imported.action == 'imported'
        assert next(restored.iter_records('attachments'))['blob_sha256'] == digest
        assert blob_path(target, digest).read_bytes() == original


def test_file_only_delta_retains_both_versions_and_uses_original_prepared_bytes(media_history):
    _, dd, cfg, _, cache, mirror, _, path, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    previous = next(cache.iter_records('attachments'))
    before = blob_path(dd, previous['blob_sha256']).read_bytes()
    filename.write_bytes(b'changed file without SQL update')
    with view(path, parent.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        assert not source.changes.pages
        child = mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z', context={'messages_media': {'format': 1}},
            validate_rows=partial(prepare_media, root=cfg.source_root('messages'), dd=dd, full=False, dirty_ids=[1]))
    filename.unlink()
    outcome = sync(media_history, child)
    assert outcome.exit_code == 0, outcome.run
    current = cache.get(previous['urn'])
    assert current['blob_sha256'] != previous['blob_sha256'] and current['availability'] == 'available'
    assert blob_path(dd, current['blob_sha256']).read_bytes() == b'changed file without SQL update'
    assert blob_path(dd, previous['blob_sha256']).read_bytes() == before
    assert len(cache.revisions_of(previous['urn'])) == 2


def test_missing_or_mismatched_point_evidence_never_reopens_live_file(media_history):
    from chatstore.adapters.base import IncompatibleSource

    _, _, _, _, _, mirror, _, _, filename, parent = media_history
    attachment = {'rowid': 1, 'filename': str(filename)}
    with pytest.raises(IncompatibleSource, match='native point'):
        read_media(mirror.conn, 'other-token', attachment)
    with pytest.raises(IncompatibleSource, match='filename'):
        read_media(mirror.conn, parent.token, attachment | {'filename': 'wrong'})
    mirror.conn.execute('DELETE FROM cs_native_media WHERE row_id=1')
    outcome = sync(media_history, parent)
    assert outcome.exit_code == 4
    assert mirror.pending() == parent


def test_media_preparation_rolls_back_with_rejected_native_point(media_history):
    _, dd, cfg, _, _, mirror, _, path, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    previous = mirror.conn.execute('SELECT observation FROM cs_native_media WHERE row_id=1').fetchone()[0]
    filename.write_bytes(b'copied before rejected publication')
    digest = hashlib.sha256(filename.read_bytes()).hexdigest()
    def reject(conn):
        prepare_media(conn, cfg.source_root('messages'), dd, full=False, dirty_ids=[1])
        raise RuntimeError('reject after retaining media')
    with (view(path, parent.cursor) as source,
          WalPages(path.with_name(path.name + '-wal'), source.changes) as pages,
          pytest.raises(RuntimeError, match='reject after')):
        mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z',
            context={'messages_media': {'format': 1}}, validate_rows=reject)
    assert mirror.pending() is None and mirror.continuation() == parent
    assert mirror.conn.execute('SELECT observation FROM cs_native_media WHERE row_id=1').fetchone()[0] == previous
    assert mirror.conn.execute('SELECT point FROM cs_native_media_state').fetchone()[0] == parent.token
    assert blob_path(dd, digest).read_bytes() == b'copied before rejected publication'


def test_native_attachment_deletion_removes_current_media_evidence(media_history):
    _, dd, cfg, _, _, mirror, writer, path, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    original = hashlib.sha256(filename.read_bytes()).hexdigest()
    writer.execute('DELETE FROM attachment WHERE ROWID=1')
    writer.execute('DELETE FROM message_attachment_join')
    writer.commit()
    with view(path, parent.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
        child = mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z',
            context={'messages_media': {'format': 1}},
            validate_rows=partial(prepare_media, root=cfg.source_root('messages'), dd=dd, full=False, dirty_ids=()))
    assert not mirror.conn.execute('SELECT 1 FROM cs_native_media WHERE row_id=1').fetchone()
    assert sync(media_history, child).exit_code == 0
    assert blob_path(dd, original).is_file()


@pytest.mark.skipif(sys.platform != 'darwin', reason='macOS source coordinator')
def test_abrupt_exit_after_media_point_commit_retries_without_source(media_history, tmp_path):
    _, dd, cfg, _, _, mirror, _, path, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    filename.write_bytes(b'prepared media before abrupt exit')
    program = """
import os, sys
from functools import partial
from pathlib import Path
from chatstore.adapters.native_mirror import NativeMirror
from chatstore.adapters.prepared_media import prepare_media
from chatstore.adapters.source_view import WalSource
from chatstore.adapters.wal import WalPages
from chatstore.paths import DataDir
source_path, mirror_path, root, data = map(Path, sys.argv[1:])
mirror = NativeMirror(mirror_path)
parent = mirror.continuation()
with WalSource(source_path) as source, source.view(parent.cursor) as view, WalPages(source_path.with_name(source_path.name + '-wal'), view.changes) as pages:
    mirror.prepare(view, pages, expected_token=parent.token, namespace=parent.namespace,
        policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z',
        context={'messages_media': {'format': 1}},
        validate_rows=partial(prepare_media, root=root, dd=DataDir(data), full=False, dirty_ids=[1]))
os._exit(23)
"""
    result = subprocess.run([sys.executable, '-c', program, str(path), str(mirror.path), str(cfg.source_root('messages')), str(dd.root)],
                            capture_output=True, text=True, timeout=15, check=False)
    assert result.returncode == 23, result.stderr
    pending = mirror.pending()
    assert pending is not None and pending.parent == parent.token
    filename.unlink()
    path.rename(tmp_path / 'offline-native.sqlite')
    outcome = sync(media_history, pending)
    assert outcome.exit_code == 0, outcome.run
    digest = hashlib.sha256(b'prepared media before abrupt exit').hexdigest()
    assert blob_path(dd, digest).read_bytes() == b'prepared media before abrupt exit'


def test_delta_requires_explicit_file_change_coverage(media_history):
    from chatstore.adapters.base import IncompatibleSource

    _, dd, cfg, _, _, mirror, _, path, _, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    with (view(path, parent.cursor) as source,
          WalPages(path.with_name(path.name + '-wal'), source.changes) as pages,
          pytest.raises(IncompatibleSource, match='complete file-change set')):
        mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
            policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z',
            context={'messages_media': {'format': 1}},
            validate_rows=partial(prepare_media, root=cfg.source_root('messages'), dd=dd, full=False))
    assert mirror.pending() is None and mirror.continuation() == parent


def _ledger(cfg, dd):
    from chatstore.adapters.media_ledger import MessagesMediaLedger

    # A new instance per attempt stands in for a new one-shot process.
    return MessagesMediaLedger(cfg.source_root('messages'), dd)


def _delta(history, previous, validator):
    """Prepare a delta, returning the point and the filenames whose bytes were read.

    Recording is scoped to the delta: a patched capture function would change
    the extraction policy that the canonical sync validates.
    """
    from chatstore.adapters import prepared_media

    *_, mirror, _, path, _, _ = history
    original = prepared_media.capture_attachment
    names: list[str] = []
    def record(*args, **kwargs):
        names.append(args[0]['filename'])
        return original(*args, **kwargs)
    prepared_media.capture_attachment = record
    try:
        with view(path, previous.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
            return mirror.prepare(source, pages, expected_token=previous.token, namespace=previous.namespace,
                policy_tag=previous.policy_tag, observed_at='2026-10-18T10:15:00Z',
                context={'messages_media': {'format': 1}}, validate_rows=validator), names
    finally:
        prepared_media.capture_attachment = original


def _coverage(mirror):
    return tuple(mirror.conn.execute('SELECT point,full FROM cs_native_media_coverage').fetchone())


def test_media_ledger_tracks_file_only_changes_across_processes_and_rejected_points(media_history):
    _, dd, cfg, _, _, mirror, _, _, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    child, captured = _delta(media_history, parent, _ledger(cfg, dd).prepare)
    assert str(filename) in captured and _coverage(mirror) == (child.token, 1)  # No ledger yet.
    assert sync(media_history, child).exit_code == 0
    clean, captured = _delta(media_history, child, _ledger(cfg, dd).prepare)
    assert captured == [] and _coverage(mirror) == (clean.token, 0)
    assert sync(media_history, clean).exit_code == 0
    filename.write_bytes(b'file-only event')
    changed, captured = _delta(media_history, clean, _ledger(cfg, dd).prepare)
    assert captured == [str(filename)] and _coverage(mirror) == (changed.token, 0)
    assert read_media(mirror.conn, changed.token, {'rowid': 1, 'filename': str(filename)})[1] == hashlib.sha256(b'file-only event').hexdigest()
    assert sync(media_history, changed).exit_code == 0
    filename.write_bytes(b'change seen by a rejected point')
    def reject(conn):
        _ledger(cfg, dd).prepare(conn)
        raise RuntimeError('publication rejected')
    with pytest.raises(RuntimeError, match='publication rejected'):
        _delta(media_history, changed, reject)
    # The rolled-back attempt also rolled back its ledger rows: the change is
    # still dirty against the parent, and nothing else is re-hashed.
    recovered, captured = _delta(media_history, changed, _ledger(cfg, dd).prepare)
    assert captured == [str(filename)] and _coverage(mirror) == (recovered.token, 0)
    assert sync(media_history, recovered).exit_code == 0


@pytest.mark.parametrize('persistent', [False, True])
def test_media_change_during_retention_retries_once_then_rejects_publication(media_history, monkeypatch, persistent):
    from chatstore.adapters import prepared_media
    from chatstore.adapters.media_capture import MediaUnavailable

    _, dd, cfg, _, _, mirror, _, _, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    child, _ = _delta(media_history, parent, _ledger(cfg, dd).prepare)
    assert sync(media_history, child).exit_code == 0
    filename.write_bytes(b'dirty before capture')
    original = prepared_media.capture_attachment
    writes = []
    def change_after_copy(attachment, *args, **kwargs):
        result = original(attachment, *args, **kwargs)
        if attachment['filename'] == str(filename) and (persistent or not writes):
            writes.append(1)
            filename.write_bytes(b'changed after retained copy %d' % len(writes))
        return result
    monkeypatch.setattr(prepared_media, 'capture_attachment', change_after_copy)
    if not persistent:
        retried, captured = _delta(media_history, child, _ledger(cfg, dd).prepare)
        assert captured == [str(filename)] * 2
        expected = hashlib.sha256(b'changed after retained copy 1').hexdigest()
        assert read_media(mirror.conn, retried.token, {'rowid': 1, 'filename': str(filename)})[1] == expected
        return
    with pytest.raises(MediaUnavailable, match='changed during capture'):
        _delta(media_history, child, _ledger(cfg, dd).prepare)
    assert len(writes) == 2
    assert mirror.pending() is None and mirror.continuation() == child
    assert mirror.conn.execute('SELECT point FROM cs_native_media_state').fetchone()[0] == child.token
    assert mirror.conn.execute('SELECT point FROM cs_native_media_ledger_state').fetchone()[0] == child.token


def _ledger_point(history):
    """Return a published, canonically acknowledged point with ledger coverage."""
    _, dd, cfg, _, _, _, _, _, _, parent = history
    assert sync(history, parent).exit_code == 0
    child, _ = _delta(history, parent, _ledger(cfg, dd).prepare)
    assert sync(history, child).exit_code == 0
    return child


@pytest.mark.parametrize('mutation', ['rewrite_same_size_restored_mtime', 'delete', 'recreate', 'symlink'])
def test_media_ledger_detects_file_mutations_without_watchers(media_history, tmp_path, mutation):
    _, dd, cfg, _, _, mirror, _, _, filename, _ = media_history
    point = _ledger_point(media_history)
    before = filename.stat()
    if mutation == 'rewrite_same_size_restored_mtime':
        filename.write_bytes(bytes(len(filename.read_bytes())))
        os.utime(filename, ns=(before.st_atime_ns, before.st_mtime_ns))
        assert filename.stat().st_mtime_ns == before.st_mtime_ns and filename.stat().st_size == before.st_size
    elif mutation == 'delete':
        filename.unlink()
    elif mutation == 'recreate':
        filename.unlink()
        point, captured = _delta(media_history, point, _ledger(cfg, dd).prepare)
        assert captured == [str(filename)]
        assert sync(media_history, point).exit_code == 0
        filename.write_bytes(b'downloaded again')
    else:
        target = tmp_path / 'elsewhere.bin'
        target.write_bytes(filename.read_bytes())
        filename.unlink()
        filename.symlink_to(target)
    changed, captured = _delta(media_history, point, _ledger(cfg, dd).prepare)
    assert captured == [str(filename)]
    availability, digest, *_ = read_media(mirror.conn, changed.token, {'rowid': 1, 'filename': str(filename)})
    if mutation == 'rewrite_same_size_restored_mtime':
        assert digest == hashlib.sha256(bytes(before.st_size)).hexdigest()
    elif mutation == 'recreate':
        assert availability == 'available' and digest == hashlib.sha256(b'downloaded again').hexdigest()
    else:
        assert availability == 'missing' and digest is None


@pytest.mark.racy_window
def test_racy_media_is_rehashed_until_its_timestamps_age(media_history, monkeypatch):
    from chatstore.adapters import media_ledger

    _, dd, cfg, _, _, mirror, _, _, filename, _ = media_history
    point = _ledger_point(media_history)  # Fixture files were written just now.
    assert mirror.conn.execute('SELECT racy FROM cs_native_media_ledger WHERE row_id=1').fetchone()[0] == 1
    point, captured = _delta(media_history, point, _ledger(cfg, dd).prepare)
    assert str(filename) in captured  # Unchanged stat, but racy: re-hashed.
    assert sync(media_history, point).exit_code == 0
    later = time.time_ns() + 2 * media_ledger.RACY_WINDOW_NS
    monkeypatch.setattr(media_ledger.time, 'time_ns', lambda: later)
    point, captured = _delta(media_history, point, _ledger(cfg, dd).prepare)
    assert str(filename) in captured  # Still racy as of the previous scan.
    assert mirror.conn.execute('SELECT racy FROM cs_native_media_ledger WHERE row_id=1').fetchone()[0] == 0
    assert sync(media_history, point).exit_code == 0
    point, captured = _delta(media_history, point, _ledger(cfg, dd).prepare)
    assert captured == [] and _coverage(mirror) == (point.token, 0)


def test_clock_rollback_reconciles_every_file(media_history, monkeypatch):
    from chatstore.adapters import media_ledger

    _, dd, cfg, _, _, mirror, _, _, filename, _ = media_history
    point = _ledger_point(media_history)
    earlier = time.time_ns() - 3600 * 10**9
    monkeypatch.setattr(media_ledger.time, 'time_ns', lambda: earlier)
    rolled_back, captured = _delta(media_history, point, _ledger(cfg, dd).prepare)
    assert str(filename) in captured and _coverage(mirror) == (rolled_back.token, 1)


def test_policy_upgrade_rebinds_retained_media_after_source_expiry(media_history, tmp_path, monkeypatch):
    from chatstore import prepared_sync

    adapter, dd, cfg, ident, cache, mirror, _, path, filename, original = media_history
    content = filename.read_bytes()
    filename.unlink()
    path.rename(tmp_path / 'offline-policy-source.sqlite')
    monkeypatch.setattr(prepared_sync, 'policy_signature', lambda *args: 'upgraded-media-policy')
    result = run_prepared_sync(adapter, dd, cfg, ident, cache, PreparedSync(mirror, original, 'chat'))
    assert result.exit_code == 0, result.run
    recovered = mirror.continuation()
    assert recovered.token != original.token and recovered.context['native_rebase_required']
    assert recovered.context['policy_recovery']['capture']['token'] == original.token
    attachment = next(cache.iter_records('attachments'))
    assert attachment['availability'] == 'available'
    assert attachment['blob_sha256'] == hashlib.sha256(content).hexdigest()
    assert blob_path(dd, attachment['blob_sha256']).read_bytes() == content
    assert mirror.conn.execute('SELECT point FROM cs_native_media_state').fetchone()[0] == recovered.token
