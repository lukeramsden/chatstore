"""Bind candidate WAL changes to a pinned, read-only SQLite source view.

Only source-side monitoring is supported on macOS. This coordinator does not
publish or acknowledge cursors; durable capture must succeed before a caller
stores the yielded cursor. Reader locks last until the context exits.
"""

from __future__ import annotations

import sqlite3
import struct
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Self

from .monitor import MonitoringUnavailable, SourceMonitor
from .wal import WalChanges, WalCursor, WalUnavailable, _checksum, read_changes


@dataclass(frozen=True)
class SourceCursor:
    database_identity: tuple[int, int]
    wal: WalCursor


@dataclass(frozen=True)
class SourceReadView:
    connection: sqlite3.Connection
    changes: WalChanges
    schema_version: int
    database_identity: tuple[int, int]
    baseline_required: bool

    @property
    def cursor(self) -> SourceCursor:
        return SourceCursor(self.database_identity, self.changes.cursor)


@dataclass(frozen=True)
class _Index:
    raw: bytes
    page_size: int
    frame: int
    database_pages: int
    checksum: tuple[int, int]
    salts: tuple[int, int]
    checksum_big_endian: bool


def _read_index(path: Path) -> _Index:
    # SQLite's WalIndexHdr: two 48-byte copies, native-endian fields except
    # the salts, which are copied byte-for-byte from the big-endian WAL header.
    with path.open('rb') as shm:
        raw = shm.read(96)
        shm.seek(0)
        repeated = shm.read(96)
    if len(raw) != 96 or raw != repeated or raw[:48] != raw[48:]:
        raise WalUnavailable('WAL index header is incomplete or changing')
    endian = '<' if sys.byteorder == 'little' else '>'
    version, _, _, initialized, big_endian, page_size, frame, pages, a, b = struct.unpack(endian + '3IBBH4I', raw[:32])
    if version != 3007000 or initialized != 1 or big_endian not in (0, 1):
        raise WalUnavailable('unsupported WAL index format')
    page_size = 65536 if page_size == 1 else page_size
    if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
        raise WalUnavailable('invalid WAL index page size')
    if _checksum(raw[:40], endian, (0, 0)) != struct.unpack(endian + '2I', raw[40:48]):
        raise WalUnavailable('invalid WAL index checksum')
    return _Index(raw[:48], page_size, frame, pages, (a, b), struct.unpack('>2I', raw[32:40]), bool(big_endian))


class WalSource:
    def __init__(self, path: Path):
        self.path = path.absolute()
        self._monitor: SourceMonitor | None = None
        try:
            self._monitor = SourceMonitor()
            self._monitor.database(self.path)
            if self._monitor.token() is None:
                raise WalUnavailable('source identity monitoring unavailable')
            stat = self.path.stat()
            self._identity = (stat.st_dev, stat.st_ino)
        except (OSError, MonitoringUnavailable) as exc:
            self.close()
            raise WalUnavailable(f'source monitoring unavailable: {type(exc).__name__}') from exc
        except BaseException:
            self.close()
            raise

    def _open(self, after: SourceCursor | None, max_frames: int | None) -> SourceReadView:
        reader: sqlite3.Connection | None = None
        try:
            if self._monitor is None:
                raise WalUnavailable('source coordinator is closed')
            before = self._monitor.token()
            if before is None:
                raise WalUnavailable('source identity monitoring failed')
            stat = self.path.stat()
            identity = (stat.st_dev, stat.st_ino)
            if identity != self._identity or (after is not None and after.database_identity != identity):
                raise WalUnavailable('source database identity changed')
            shm = self.path.with_name(self.path.name + '-shm')
            index = _read_index(shm)
            reader = sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True, timeout=1)
            reader.execute('PRAGMA query_only=1')
            reader.execute('BEGIN')
            reader.execute('SELECT rootpage FROM sqlite_schema LIMIT 1').fetchone()
            if _read_index(shm) != index or self._monitor.token() != before:
                raise WalUnavailable('source changed while pinning SQLite view')
            if reader.execute('PRAGMA page_count').fetchone()[0] != index.database_pages:
                raise WalUnavailable('SQLite view and WAL index page counts differ')
            schema_version = reader.execute('PRAGMA schema_version').fetchone()[0]
            changes = read_changes(self.path.with_name(self.path.name + '-wal'),
                after.wal if after else None, max_frames=max_frames, through_frame=index.frame)
            cursor = changes.cursor
            if (cursor.page_size, cursor.salts, cursor.checksum, cursor.database_pages, changes.checksum_big_endian) != (
                    index.page_size, index.salts, index.checksum, index.database_pages, index.checksum_big_endian):
                raise WalUnavailable('WAL candidate does not match pinned source view')
            # Commits after the view was pinned remain beyond this cursor. They
            # need not abort capture, but an identity/monitoring failure must.
            if self._monitor.token() is None:
                raise WalUnavailable('source identity changed while validating WAL candidate')
            return SourceReadView(reader, changes, schema_version, identity, after is None)
        except (OSError, sqlite3.Error) as exc:
            if reader is not None:
                reader.close()
            raise WalUnavailable(f'source view unavailable: {type(exc).__name__}') from exc
        except BaseException:
            if reader is not None:
                reader.close()
            raise

    @contextmanager
    def view(self, after: SourceCursor | None = None, *, max_frames: int | None = 50000):
        view = self._open(after, max_frames)
        try:
            yield view
        finally:
            try:
                view.connection.rollback()
            finally:
                view.connection.close()

    def close(self) -> None:
        if self._monitor is not None:
            self._monitor.close()
            self._monitor = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
