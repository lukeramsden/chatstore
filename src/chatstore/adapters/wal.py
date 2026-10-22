"""Validated SQLite WAL page changes; never an independent source-capture watermark.

A caller must coordinate this candidate with a stable SQLite read transaction and
commit it only after durable capture. Reset/unavailable logs require reconciliation.
Change discovery retains frame descriptors, never page bodies. A separate reader
can retrieve verified committed pages while the coordinated source view stays pinned.
"""

from __future__ import annotations

import os
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Self


class WalUnavailable(Exception):
    """The log cannot prove continuity; use a consistent full source capture."""


@dataclass(frozen=True)
class WalCursor:
    device: int
    inode: int
    page_size: int
    salts: tuple[int, int]
    frame: int
    checksum: tuple[int, int]
    database_pages: int | None


@dataclass(frozen=True)
class WalFrame:
    number: int
    page: int
    database_pages: int
    prior_checksum: tuple[int, int]
    checksum: tuple[int, int]


@dataclass(frozen=True)
class WalChanges:
    cursor: WalCursor
    pages: frozenset[int]
    commits: int
    pending_frames: int
    frames_read: int
    bytes_read: int
    checksum_big_endian: bool
    frames: Mapping[int, WalFrame]
    after: WalCursor | None


def _checksum(data: bytes, endian: str, prior: tuple[int, int]) -> tuple[int, int]:
    first, second = prior
    for a, b in struct.iter_unpack(endian + 'II', data):
        first = (first + a + second) & 0xFFFFFFFF
        second = (second + b + first) & 0xFFFFFFFF
    return first, second


def read_changes(path: Path, after: WalCursor | None = None, *, max_frames: int | None = 50000,
                 through_frame: int | None = None) -> WalChanges:
    """Read committed page numbers after a validated cursor, using bounded frame buffers.

    Valid uncommitted tail frames do not advance the returned cursor. Partial or
    invalid frames reject the candidate. Bytes appended beyond the sampled extent
    are left for the next coordinated read, not silently included in its watermark.
    """
    try:
        with path.open('rb') as wal:
            stat = os.fstat(wal.fileno())
            header = wal.read(32)
            if len(header) != 32:
                raise WalUnavailable('missing WAL header')
            magic, version, page_size, _, salt1, salt2, check1, check2 = struct.unpack('>8I', header)
            if magic not in (0x377F0682, 0x377F0683) or version != 3007000:
                raise WalUnavailable('unsupported WAL format')
            if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
                raise WalUnavailable('invalid WAL page size')
            endian = '<' if magic == 0x377F0682 else '>'
            running = _checksum(header[:24], endian, (0, 0))
            if running != (check1, check2):
                raise WalUnavailable('invalid WAL header checksum')
            salts = (salt1, salt2)
            frame_size = 24 + page_size
            extent = stat.st_size - 32
            if extent < 0:
                raise WalUnavailable('missing WAL extent')
            if through_frame is None:
                if extent % frame_size:
                    raise WalUnavailable('partial WAL frame')
                frames = extent // frame_size
            else:
                if not 0 <= through_frame <= extent // frame_size:
                    raise WalUnavailable('WAL bound is outside sampled extent')
                frames = through_frame
            start = after.frame if after else 0
            if max_frames is not None and frames - start > max_frames:
                raise WalUnavailable('WAL change batch exceeds frame limit')
            cursor = WalCursor(stat.st_dev, stat.st_ino, page_size, salts, 0, running, None)
            bytes_read = 32
            if after:
                if (after.device, after.inode, after.page_size, after.salts) != (
                        stat.st_dev, stat.st_ino, page_size, salts) or not 0 <= start <= frames:
                    raise WalUnavailable('WAL reset or replaced')
                if start:
                    wal.seek(32 + (start - 1) * frame_size)
                    previous = wal.read(24)
                    bytes_read += len(previous)
                    if len(previous) != 24:
                        raise WalUnavailable('WAL cursor frame disappeared')
                    _, committed_size, a, b, c, d = struct.unpack('>6I', previous)
                    if (a, b) != salts or (c, d) != after.checksum or committed_size != after.database_pages or not committed_size:
                        raise WalUnavailable('WAL cursor no longer identifies its committed frame')
                elif after.checksum != running or after.database_pages is not None:
                    raise WalUnavailable('invalid initial WAL cursor')
                cursor, running = after, after.checksum
            wal.seek(32 + start * frame_size)
            changed: set[int] = set()
            pending: dict[int, WalFrame] = {}
            committed: dict[int, WalFrame] = {}
            pending_frames = commits = 0
            for frame in range(start + 1, frames + 1):
                raw = wal.read(frame_size)
                bytes_read += len(raw)
                if len(raw) != frame_size:
                    raise WalUnavailable('WAL changed during read')
                page, database_pages, a, b, c, d = struct.unpack('>6I', raw[:24])
                if not 0 < page <= 0xFFFFFFFE or database_pages > 0xFFFFFFFE or (a, b) != salts:
                    raise WalUnavailable('invalid WAL frame identity')
                prior_checksum = running
                running = _checksum(raw[:8], endian, running)
                running = _checksum(raw[24:], endian, running)
                if running != (c, d):
                    raise WalUnavailable('invalid WAL frame checksum')
                pending[page] = WalFrame(frame, page, database_pages, prior_checksum, running)
                pending_frames += 1
                if database_pages:
                    changed.update(pending)
                    committed.update(pending)
                    pending.clear()
                    pending_frames = 0
                    commits += 1
                    cursor = WalCursor(stat.st_dev, stat.st_ino, page_size, salts, frame, running, database_pages)
            if through_frame is not None and cursor.frame != through_frame:
                raise WalUnavailable('WAL bound is not a committed frame')
            wal.seek(0)
            final_header = wal.read(32)
            bytes_read += len(final_header)
            final_stat = os.fstat(wal.fileno())
            named_stat = path.stat()
            if ((named_stat.st_dev, named_stat.st_ino) != (stat.st_dev, stat.st_ino)
                    or final_header != header or final_stat.st_size < stat.st_size):
                raise WalUnavailable('WAL reset during read')
            return WalChanges(cursor, frozenset(changed), commits, pending_frames, frames - start, bytes_read, magic == 0x377F0683, MappingProxyType(committed), after)
    except OSError as exc:
        raise WalUnavailable(f'WAL unavailable: {type(exc).__name__}') from exc


class WalPages:
    """Read only committed changed-page bodies bound to a validated WAL candidate.

    This object does not pin SQLite or authorize source cursor publication. Keep
    the matching SourceReadView open throughout use. Ordinary later WAL appends
    are allowed; reset, replacement and altered selected frames are rejected.
    """

    def __init__(self, path: Path, changes: WalChanges):
        self.path = path
        self.changes = changes
        self.bytes_read = 0
        self.pages_read = 0
        try:
            self._file = path.open('rb')
            try:
                self._header = self._read(32, 0)
                cursor = changes.cursor
                if len(self._header) != 32:
                    raise WalUnavailable('missing WAL page header')
                magic, version, size, _, a, b, c, d = struct.unpack('>8I', self._header)
                self._endian = '>' if changes.checksum_big_endian else '<'
                if (magic != (0x377F0683 if changes.checksum_big_endian else 0x377F0682)
                        or version != 3007000 or size != cursor.page_size or (a, b) != cursor.salts
                        or _checksum(self._header[:24], self._endian, (0, 0)) != (c, d)):
                    raise WalUnavailable('WAL page header does not match candidate')
                self.check_stable()
            except BaseException:
                self._file.close()
                raise
        except OSError as exc:
            raise WalUnavailable(f'WAL pages unavailable: {type(exc).__name__}') from exc

    def _read(self, size: int, offset: int) -> bytes:
        data = os.pread(self._file.fileno(), size, offset)
        self.bytes_read += len(data)
        return data

    def read(self, page: int) -> bytes:
        try:
            descriptor = self.changes.frames.get(page)
            cursor = self.changes.cursor
            if descriptor is None or descriptor.page != page or not 0 < descriptor.number <= cursor.frame:
                raise WalUnavailable('page has no committed frame in this candidate')
            size = cursor.page_size
            raw = self._read(size + 24, 32 + (descriptor.number - 1) * (size + 24))
            if len(raw) != size + 24:
                raise WalUnavailable('selected WAL frame disappeared')
            number, database_pages, a, b, c, d = struct.unpack('>6I', raw[:24])
            checksum = _checksum(raw[:8], self._endian, descriptor.prior_checksum)
            checksum = _checksum(raw[24:], self._endian, checksum)
            if ((number, database_pages, (a, b), (c, d)) != (
                    page, descriptor.database_pages, cursor.salts, descriptor.checksum)
                    or checksum != descriptor.checksum):
                raise WalUnavailable('selected WAL frame changed')
            self.check_stable()
            self.pages_read += 1
            return raw[24:]
        except OSError as exc:
            raise WalUnavailable(f'WAL page unavailable: {type(exc).__name__}') from exc

    def check_stable(self) -> None:
        try:
            cursor = self.changes.cursor
            stat = os.fstat(self._file.fileno())
            named = self.path.stat()
            if ((stat.st_dev, stat.st_ino) != (cursor.device, cursor.inode)
                    or (named.st_dev, named.st_ino) != (cursor.device, cursor.inode)
                    or self._read(32, 0) != self._header
                    or stat.st_size < 32 + cursor.frame * (cursor.page_size + 24)):
                raise WalUnavailable('WAL pages reset or replaced')
            if cursor.frame:
                raw = self._read(24, 32 + (cursor.frame - 1) * (cursor.page_size + 24))
                if len(raw) != 24:
                    raise WalUnavailable('WAL candidate commit disappeared')
                _, committed, a, b, c, d = struct.unpack('>6I', raw)
                if ((a, b), (c, d), committed) != (cursor.salts, cursor.checksum, cursor.database_pages):
                    raise WalUnavailable('WAL candidate commit changed')
        except OSError as exc:
            raise WalUnavailable(f'WAL pages unavailable: {type(exc).__name__}') from exc

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
