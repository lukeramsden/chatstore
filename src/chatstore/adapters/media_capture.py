"""Retain stable source-file bytes before publishing prepared media evidence."""

from __future__ import annotations

import errno
import hashlib
import os
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..paths import DataDir, blob_path


class MediaUnavailable(Exception):
    """File bytes could not be bound to one stable observation; retry capture."""


@dataclass(frozen=True)
class CapturedMedia:
    filename: str | None
    availability: str
    sha256: str | None
    size: int | None
    hint: str | None

    @property
    def probe(self) -> tuple[str, str | None, int | None, str | None]:
        return self.availability, self.sha256, self.size, self.hint


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _publish(dd: DataDir, temporary: Path, digest: str, size: int) -> None:
    target = blob_path(dd, digest)
    try:
        target.parent.mkdir(mode=0o700)
    except FileExistsError:
        pass
    # A previous failed attempt may have created an unflushed bucket.
    _sync_directory(dd.blobs)
    try:
        os.link(temporary, target)
    except FileExistsError:
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as existing:
            before = os.fstat(existing.fileno())
            if (not stat.S_ISREG(before.st_mode) or before.st_size != size
                    or hashlib.file_digest(existing, 'sha256').hexdigest() != digest
                    or _identity(before) != _identity(os.fstat(existing.fileno()))
                    or _identity(before) != _identity(target.lstat())):
                raise MediaUnavailable('existing retained media does not match its content address')
            os.fsync(existing.fileno())
    _sync_directory(target.parent)


def capture_attachment(a: dict[str, Any], root: Path, dd: DataDir) -> CapturedMedia:
    """Copy/hash once, fsync, and publish without replacing prior retained bytes.

    The data directory must already be initialized. Returned available evidence is independent of subsequent source disappearance.
    A failed call may leave an unreferenced content-addressed blob, never a receipt.
    """
    filename, size = a.get('filename'), a.get('total_bytes')
    if a.get('_purged'):
        return CapturedMedia(filename, 'missing', None, size, None)
    if not filename:
        return CapturedMedia(filename, 'not_downloaded', None, size, None)
    path = Path(filename).expanduser()
    try:
        hint = str(path.relative_to(root))
    except ValueError:
        hint = os.path.basename(filename)
    absent = CapturedMedia(filename, 'missing' if filename.startswith(('~', '/')) else 'unknown', None, size, hint)
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as exc:
        if exc.errno in (errno.ENOENT, errno.ENOTDIR, errno.ELOOP):
            return absent
        raise MediaUnavailable(f'cannot open source media: errno {exc.errno}') from exc
    temporary: Path | None = None
    try:
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            return absent
        target_fd, name = tempfile.mkstemp(prefix='.capture-', dir=dd.blobs)
        temporary = Path(name)
        digest = hashlib.sha256()
        total = 0
        with os.fdopen(target_fd, 'wb') as target:
            while chunk := os.read(fd, 1 << 20):
                digest.update(chunk)
                target.write(chunk)
                total += len(chunk)
            target.flush()
            os.fsync(target.fileno())
        try:
            current = path.lstat()
        except OSError as exc:
            raise MediaUnavailable('source media disappeared during capture') from exc
        if (total != before.st_size or _identity(before) != _identity(os.fstat(fd))
                or _identity(before) != _identity(current) or not stat.S_ISREG(current.st_mode)):
            raise MediaUnavailable('source media changed during capture')
        value = digest.hexdigest()
        _publish(dd, temporary, value, total)
        return CapturedMedia(filename, 'available', value, total, hint)
    finally:
        os.close(fd)
        if temporary is not None:
            temporary.unlink(missing_ok=True)
