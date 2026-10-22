import hashlib
import json
import os
import subprocess
import sys
from dataclasses import asdict

import pytest

from chatstore.adapters import media_capture
from chatstore.adapters.media_capture import MediaUnavailable, capture_attachment
from chatstore.adapters.messages.adapter import _probe_attachment
from chatstore.paths import DataDir, blob_path

pytestmark = pytest.mark.skipif(sys.platform == 'win32', reason='POSIX file-descriptor capture')


@pytest.fixture
def media(tmp_path):
    dd = DataDir(tmp_path / 'data')
    dd.create()
    root = tmp_path / 'messages'
    root.mkdir()
    path = root / 'attachment.bin'
    path.write_bytes(b'preserved media\0\xff' * 200000)
    return dd, root, path


def test_capture_retains_identical_probe_and_bytes_after_source_disappears(media):
    dd, root, path = media
    attachment = {'filename': str(path), 'total_bytes': 1}
    expected = _probe_attachment(attachment, root)
    captured = capture_attachment(attachment, root, dd)
    assert captured.probe == expected
    saved = blob_path(dd, captured.sha256)
    original = path.read_bytes()
    assert saved.read_bytes() == original
    assert saved.stat().st_mode & 0o777 == 0o600
    inode = saved.stat().st_ino
    assert capture_attachment(attachment, root, dd) == captured
    assert saved.stat().st_ino == inode
    path.unlink()
    assert saved.read_bytes() == original
    assert captured.availability == 'available'
    assert not list(dd.blobs.glob('.capture-*'))


@pytest.mark.parametrize('kind', ['absent', 'relative', 'no-filename', 'purged', 'symlink', 'directory', 'fifo'])
def test_unavailable_probe_matches_existing_adapter_without_retaining_bytes(media, kind):
    dd, root, path = media
    attachment = {'filename': str(path), 'total_bytes': 17}
    if kind == 'absent':
        path.unlink()
    elif kind == 'relative':
        attachment['filename'] = 'missing-relative-file'
    elif kind == 'no-filename':
        attachment['filename'] = None
    elif kind == 'purged':
        attachment['_purged'] = True
    elif kind == 'symlink':
        link = root / 'link'
        link.symlink_to(path)
        attachment['filename'] = str(link)
    elif kind == 'directory':
        attachment['filename'] = str(root)
    else:
        path.unlink()
        os.mkfifo(path)
    captured = capture_attachment(attachment, root, dd)
    assert captured.probe == _probe_attachment(attachment, root)
    assert captured.sha256 is None
    assert not list(dd.blobs.rglob('*'))


@pytest.mark.parametrize('change', ['overwrite', 'replace', 'unlink'])
def test_mutation_during_copy_cannot_publish_available_evidence(media, monkeypatch, change):
    dd, root, path = media
    original = hashlib.sha256
    class ChangingHash:
        def __init__(self):
            self.value = original()
            self.changed = False

        def update(self, chunk):
            self.value.update(chunk)
            if not self.changed:
                self.changed = True
                if change == 'overwrite':
                    with path.open('r+b') as writer:
                        writer.write(b'changed')
                elif change == 'replace':
                    replacement = root / 'replacement'
                    replacement.write_bytes(b'replaced')
                    replacement.replace(path)
                else:
                    path.unlink()

        def hexdigest(self):
            return self.value.hexdigest()
    monkeypatch.setattr(media_capture.hashlib, 'sha256', ChangingHash)
    with pytest.raises(MediaUnavailable, match='during capture'):
        capture_attachment({'filename': str(path)}, root, dd)
    assert not list(dd.blobs.rglob('*'))


def test_corrupt_existing_blob_is_never_overwritten_or_accepted(media):
    dd, root, path = media
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    saved = blob_path(dd, digest)
    saved.parent.mkdir()
    saved.write_bytes(b'corrupt prior blob')
    inode = saved.stat().st_ino
    with pytest.raises(MediaUnavailable, match='content address'):
        capture_attachment({'filename': str(path)}, root, dd)
    assert saved.read_bytes() == b'corrupt prior blob' and saved.stat().st_ino == inode
    assert not list(dd.blobs.glob('.capture-*'))


def test_directory_fsync_failure_does_not_return_evidence_and_retry_is_safe(media, monkeypatch):
    dd, root, path = media
    attachment = {'filename': str(path)}
    sync = media_capture._sync_directory
    def fail(directory):
        raise OSError('injected durable publication failure')
    monkeypatch.setattr(media_capture, '_sync_directory', fail)
    with pytest.raises(OSError, match='publication failure'):
        capture_attachment(attachment, root, dd)
    assert not list(dd.blobs.glob('.capture-*'))
    monkeypatch.setattr(media_capture, '_sync_directory', sync)
    captured = capture_attachment(attachment, root, dd)
    assert blob_path(dd, captured.sha256).read_bytes() == path.read_bytes()


def test_abrupt_exit_after_capture_leaves_independent_blob_and_serializable_evidence(media, tmp_path):
    dd, root, path = media
    evidence = tmp_path / 'evidence.json'
    program = '''
import json, os, sys
from dataclasses import asdict
from pathlib import Path
from chatstore.adapters.media_capture import capture_attachment
from chatstore.paths import DataDir
source, root, data, evidence = map(Path, sys.argv[1:])
captured = capture_attachment({'filename': str(source)}, root, DataDir(data))
with evidence.open('w') as output:
    json.dump(asdict(captured), output)
    output.flush()
    os.fsync(output.fileno())
os._exit(23)
'''
    expected = path.read_bytes()
    result = subprocess.run([sys.executable, '-c', program, str(path), str(root), str(dd.root), str(evidence)],
                            capture_output=True, text=True, timeout=15, check=False)
    assert result.returncode == 23, result.stderr
    path.unlink()
    captured = media_capture.CapturedMedia(**json.loads(evidence.read_text()))
    assert asdict(captured)['availability'] == 'available'
    assert blob_path(dd, captured.sha256).read_bytes() == expected


def test_source_bytes_are_read_once_and_empty_files_are_retained(media, monkeypatch):
    dd, root, path = media
    read = os.read
    consumed = []
    def counted(fd, size):
        chunk = read(fd, size)
        consumed.append(len(chunk))
        return chunk
    monkeypatch.setattr(media_capture.os, 'read', counted)
    expected_size = path.stat().st_size
    captured = capture_attachment({'filename': str(path)}, root, dd)
    assert sum(consumed) == expected_size == captured.size
    path.write_bytes(b'')
    empty = capture_attachment({'filename': str(path)}, root, dd)
    assert empty.size == 0 and empty.availability == 'available'
    assert empty.sha256 == hashlib.sha256(b'').hexdigest()
    assert blob_path(dd, empty.sha256).read_bytes() == b''
