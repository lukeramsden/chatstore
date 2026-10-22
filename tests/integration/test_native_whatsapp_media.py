import hashlib
import os
import sqlite3
import sys
from contextlib import closing
from pathlib import Path

import pytest

from chatstore.adapters.whatsapp import media
from chatstore.adapters.whatsapp.adapter import WhatsAppAdapter
from chatstore.archive.import_ import import_archive
from chatstore.archive.incremental import export_changes
from chatstore.cache import Cache
from chatstore.config import Config, Identity
from chatstore.native_collector import collect_native
from chatstore.paths import DataDir, blob_path
from chatstore.sync_session import SyncSession
from tests.fixtures.synthetic import build_whatsapp

pytestmark = pytest.mark.skipif(sys.platform != 'darwin', reason='macOS native media monitoring')


@pytest.fixture
def history(tmp_path):
    root = build_whatsapp(tmp_path / 'source', extra_messages=50)
    dd = DataDir(tmp_path / 'data')
    dd.create()
    cfg, ident = Config(source_paths={'whatsapp': str(root)}), Identity()
    ident.ensure_scope('whatsapp')
    with closing(sqlite3.connect(root / 'ChatStorage.sqlite')) as writer:
        writer.execute('PRAGMA journal_mode=WAL')
        writer.execute('PRAGMA wal_autocheckpoint=0')
        writer.execute('PRAGMA user_version=1')
        writer.commit()
        with closing(Cache(dd.cache)) as cache, SyncSession() as session:
            def poll():
                result = collect_native(WhatsAppAdapter(), dd, cfg, ident, cache,
                    auxiliary_session=session.native_auxiliary(cfg),
                    media_watch=session.native_media(cfg, dd, source='whatsapp'))
                assert result is not None and result.exit_code == 0, result
                return result.run
            yield root, dd, cfg, ident, writer, cache, session, poll


def test_shared_file_changes_missing_and_downloads_cold_restore_all_versions(history, tmp_path, monkeypatch):
    root, dd, cfg, ident, writer, cache, _, poll = history
    writer.execute("INSERT INTO ZWAMEDIAITEM(Z_PK,ZMESSAGE,ZMEDIALOCALPATH,ZFILESIZE) VALUES(2,6,'Media/x/pic.jpg',9)")
    writer.commit()
    copied = []
    original = media.capture_attachment
    def capture(*args, **kwargs):
        copied.append(args[0]['filename'])
        return original(*args, **kwargs)
    monkeypatch.setattr(media, 'capture_attachment', capture)
    baseline = poll()
    assert baseline['snapshot']['prepared_native']['capture']['context']['whatsapp_media'] == {'format': 1}
    assert len(copied) == 2
    attachments = list(cache.iter_records('attachments'))
    assert len(attachments) == 2 and all(row['availability'] == 'available' for row in attachments)
    assert poll()['counts']['prepared_coverage_reused'] == 1 and len(copied) == 2
    archives = []
    def export():
        result = export_changes(cache, dd, cfg, ident, consumer='media-restore', output=dd.exports,
                                password='test-password', media='available-media')
        assert result['action'] == 'written' and not result['has_more']
        archives.append(Path(result['path']))
    export()
    path = root / 'Message/Media/x/pic.jpg'
    versions = {hashlib.sha256(path.read_bytes()).hexdigest()}
    with path.open('r+b', buffering=0) as file:
        file.write(b'changed through existing descriptor')
        file.truncate()
        os.fsync(file.fileno())
    for availability in ('available', 'missing', 'available'):
        changed = poll()
        assert changed['counts']['prepared_headers_reused'] == 1
        assert changed['counts']['messages'] == 2
        assert changed['snapshot']['prepared_native']['capture']['rows_changed'] == 0
        rows = list(cache.iter_records('attachments'))
        assert all(row['availability'] == availability for row in rows)
        if availability == 'available':
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            versions.add(digest)
            assert all(row['blob_sha256'] == digest for row in rows)
            assert blob_path(dd, digest).read_bytes() == path.read_bytes()
        export()
        if availability == 'missing':
            path.write_bytes(b'later re-download')
        else:
            path.unlink()
    assert len(copied) == 8 and len(versions) == 3
    expected = {row['urn']: {revision['revision_digest'] for revision in cache.revisions_of(row['urn'])}
                for row in attachments}
    assert all(len(digests) == 4 for digests in expected.values())
    root.rename(tmp_path / 'offline-source')
    dd.blobs.rename(tmp_path / 'offline-producer-blobs')
    restored = DataDir(tmp_path / 'cold-restore')
    restored.create()
    with closing(Cache(restored.cache)) as target:
        target_ident = Identity()
        for archive in archives:
            assert import_archive(target, restored, target_ident, archive, 'test-password').action == 'imported'
        for urn, digests in expected.items():
            assert {revision['revision_digest'] for revision in target.revisions_of(urn)} == digests
        for digest in versions:
            assert blob_path(restored, digest).is_file()


def test_failed_file_capture_cannot_publish_or_fall_back_and_restart_reconciles(history, monkeypatch):
    from chatstore.adapters.media_capture import MediaUnavailable
    from chatstore.adapters.native_mirror import NativeMirror
    from chatstore.adapters.whatsapp.media_watch import PreparedWhatsAppMediaWatch

    root, dd, cfg, _, _, cache, session, poll = history
    fail = [False]
    original = media.capture_attachment
    def capture(*args, **kwargs):
        if fail[0]:
            raise MediaUnavailable('injected WhatsApp file capture failure')
        return original(*args, **kwargs)
    monkeypatch.setattr(media, 'capture_attachment', capture)
    initial = poll()['snapshot']['prepared_native']['capture']
    previous = next(cache.iter_records('attachments'))
    (root / 'Message/Media/x/pic.jpg').write_bytes(b'changed while capture fails')
    fail[0] = True
    with pytest.raises(MediaUnavailable, match='injected WhatsApp'):
        poll()
    watcher = session.native_media(cfg, dd, source='whatsapp')
    assert isinstance(watcher, PreparedWhatsAppMediaWatch) and watcher._monitor is None
    with NativeMirror(next((dd.root / 'native').glob('*.sqlite3'))) as mirror:
        assert mirror.pending() is None and mirror.continuation().token == initial['token']
    assert next(cache.iter_records('attachments'))['revision_digest'] == previous['revision_digest']
    fail[0] = False
    recovered = poll()
    assert recovered['mode'] == 'full_reconcile'
    assert next(cache.iter_records('attachments'))['blob_sha256'] != previous['blob_sha256']
    assert blob_path(dd, previous['blob_sha256']).is_file()
    assert poll()['counts']['prepared_coverage_reused'] == 1


@pytest.mark.parametrize('upgrade', [False, True])
def test_pending_whatsapp_media_retries_after_source_expiry_and_policy_upgrade(history, monkeypatch, tmp_path, upgrade):
    from chatstore import prepared_sync
    from chatstore.adapters.native_mirror import NativeMirror

    root, dd, _, _, _, cache, _, poll = history
    poll()
    path = root / 'Message/Media/x/pic.jpg'
    captured = b'bytes captured before expiry'
    path.write_bytes(captured)
    def interrupt(*args, **kwargs):
        raise RuntimeError('interrupt canonical WhatsApp media')
    with monkeypatch.context() as patch:
        patch.setattr(cache, 'upsert_many', interrupt)
        with pytest.raises(RuntimeError, match='interrupt canonical'):
            poll()
    mirror_path = next((dd.root / 'native').glob('*.sqlite3'))
    with NativeMirror(mirror_path) as mirror:
        pending = mirror.pending()
        assert pending is not None
    root.rename(tmp_path / 'expired-source')
    if upgrade:
        monkeypatch.setattr(prepared_sync, 'policy_signature', lambda *args: 'new-wa-media-policy')
    retried = poll()
    capture = retried['snapshot']['prepared_native']['capture']
    assert capture['observed_at'] == pending.observed_at
    assert (capture['token'] != pending.token) is upgrade
    attachment = next(cache.iter_records('attachments'))
    digest = hashlib.sha256(captured).hexdigest()
    assert attachment['availability'] == 'available' and attachment['blob_sha256'] == digest
    assert blob_path(dd, digest).read_bytes() == captured
    with NativeMirror(mirror_path) as mirror:
        assert mirror.pending() is None and mirror.continuation().token == capture['token']
        assert mirror.conn.execute('SELECT point FROM cs_native_wa_media_state').fetchone()[0] == capture['token']
        if upgrade:
            assert mirror.continuation().context['native_rebase_required']


def test_new_filename_late_download_and_retired_file_watch(history):
    root, _, _, _, writer, cache, _, poll = history
    poll()
    old = root / 'Message/Media/x/pic.jpg'
    writer.execute("UPDATE ZWAMEDIAITEM SET ZMEDIALOCALPATH='Media/new-file' WHERE Z_PK=1")
    writer.commit()
    assert poll()['counts']['messages'] == 1
    assert next(cache.iter_records('attachments'))['availability'] == 'missing'
    new = root / 'Message/Media/new-file'
    new.write_bytes(b'late new filename download')
    downloaded = poll()
    assert downloaded['counts']['messages'] == 1
    assert downloaded['snapshot']['prepared_native']['capture']['rows_changed'] == 0
    assert next(cache.iter_records('attachments'))['blob_sha256'] == hashlib.sha256(new.read_bytes()).hexdigest()
    old.write_bytes(b'a retired file changed')
    assert poll()['counts']['prepared_coverage_reused'] == 1
