import hashlib
import sqlite3
import subprocess
import sys
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


@pytest.mark.skipif(sys.platform != 'darwin', reason='macOS vnode monitoring')
def test_live_media_watch_tracks_file_only_changes_and_recovers_rejected_point(media_history, monkeypatch):
    from chatstore.adapters import prepared_media
    from chatstore.adapters.media_watch import PreparedMediaWatch

    _, dd, cfg, _, _, mirror, _, path, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    watcher = PreparedMediaWatch(cfg.source_root('messages'), dd)
    def prepare(previous, validator):
        with view(path, previous.cursor) as source, WalPages(path.with_name(path.name + '-wal'), source.changes) as pages:
            return mirror.prepare(source, pages, expected_token=previous.token, namespace=previous.namespace,
                policy_tag=previous.policy_tag, observed_at='2026-10-18T10:15:00Z',
                context={'messages_media': {'format': 1}}, validate_rows=validator)
    try:
        child = prepare(parent, watcher.prepare)
        assert sync(media_history, child).exit_code == 0
        original = prepared_media.capture_attachment
        captured = []
        def record(*args, **kwargs):
            captured.append(args[0]['filename'])
            return original(*args, **kwargs)
        monkeypatch.setattr(prepared_media, 'capture_attachment', record)
        clean = prepare(child, watcher.prepare)
        assert captured == []
        monkeypatch.setattr(prepared_media, 'capture_attachment', original)
        assert sync(media_history, clean).exit_code == 0
        monkeypatch.setattr(prepared_media, 'capture_attachment', record)
        filename.write_bytes(b'file-only event')
        changed = prepare(clean, watcher.prepare)
        assert captured == [str(filename)]
        assert read_media(mirror.conn, changed.token, {'rowid': 1, 'filename': str(filename)})[1] == hashlib.sha256(b'file-only event').hexdigest()
        monkeypatch.setattr(prepared_media, 'capture_attachment', original)
        assert sync(media_history, changed).exit_code == 0
        monkeypatch.setattr(prepared_media, 'capture_attachment', record)
        def reject(conn):
            watcher.prepare(conn)
            raise RuntimeError('publication rejected')
        with pytest.raises(RuntimeError, match='publication rejected'):
            prepare(changed, reject)
        captured.clear()
        recovered = prepare(changed, watcher.prepare)
        assert str(filename) in captured  # Rolled-back point forces full file reconciliation.
        monkeypatch.setattr(prepared_media, 'capture_attachment', original)
        assert sync(media_history, recovered).exit_code == 0
    finally:
        watcher.close()


@pytest.mark.skipif(sys.platform != 'darwin', reason='macOS vnode monitoring')
def test_live_media_change_during_retention_rejects_native_publication(media_history, monkeypatch):
    from chatstore.adapters import prepared_media
    from chatstore.adapters.media_capture import MediaUnavailable
    from chatstore.adapters.media_watch import PreparedMediaWatch

    _, dd, cfg, _, _, mirror, _, path, filename, parent = media_history
    assert sync(media_history, parent).exit_code == 0
    watcher = PreparedMediaWatch(cfg.source_root('messages'), dd)
    original = prepared_media.capture_attachment
    def change_after_copy(attachment, *args, **kwargs):
        captured = original(attachment, *args, **kwargs)
        if attachment['filename'] == str(filename):
            filename.write_bytes(b'changed after retained copy')
        return captured
    monkeypatch.setattr(prepared_media, 'capture_attachment', change_after_copy)
    try:
        with (view(path, parent.cursor) as source,
              WalPages(path.with_name(path.name + '-wal'), source.changes) as pages,
              pytest.raises(MediaUnavailable, match='changed during')):
            mirror.prepare(source, pages, expected_token=parent.token, namespace=parent.namespace,
                policy_tag=parent.policy_tag, observed_at='2026-10-18T10:15:00Z',
                context={'messages_media': {'format': 1}}, validate_rows=watcher.prepare)
        assert mirror.pending() is None
        assert mirror.continuation() == parent
        assert mirror.conn.execute('SELECT point FROM cs_native_media_state').fetchone()[0] == parent.token
        assert watcher._binding is None
    finally:
        watcher.close()
