"""Bounded-buffer row-ID decoding of private, consistent SQLite page images.

Only rowid-table B-trees are supported. Unsupported or malformed images require
ordinary source reconciliation. Payload bytes are never decoded or retained in returned metadata.
"""

from __future__ import annotations

import os
import struct
from collections.abc import Iterator
from dataclasses import dataclass
from itertools import pairwise
from pathlib import Path
from typing import Self


class PageUnavailable(Exception):
    pass


@dataclass(frozen=True)
class Cell:
    row_id: int
    payload_size: int
    local_size: int
    overflow: int | None


@dataclass(frozen=True)
class TreePage:
    number: int
    children: tuple[int, ...]
    cells: tuple[Cell, ...]
    separators: tuple[int, ...]


@dataclass(frozen=True)
class RowLocation:
    page: int
    row_id: int
    overflow: tuple[int, ...]


def _varint(data: bytes, offset: int, end: int) -> tuple[int, int]:
    value = 0
    for _ in range(8):
        if offset >= end:
            raise PageUnavailable('truncated SQLite varint')
        byte = data[offset]
        offset += 1
        value = (value << 7) | (byte & 0x7F)
        if byte < 128:
            return value, offset
    if offset >= end:
        raise PageUnavailable('truncated SQLite varint')
    return (value << 8) | data[offset], offset + 1


def _row_id(value: int) -> int:
    return value - (1 << 64) if value >= (1 << 63) else value


def decode_tree_page(data: bytes, page: int, *, usable: int, page_count: int) -> TreePage:
    if len(data) < usable or not 1 <= page <= page_count:
        raise PageUnavailable('invalid SQLite page buffer or number')
    start = 100 if page == 1 else 0
    kind = data[start]
    if kind not in (5, 13):
        raise PageUnavailable('not a rowid-table B-tree page')
    header_size = 12 if kind == 5 else 8
    count = int.from_bytes(data[start + 3:start + 5], 'big')
    pointers_end = start + header_size + count * 2
    if pointers_end > usable or data[start + 7] > 60:
        raise PageUnavailable('invalid SQLite B-tree header')
    area = int.from_bytes(data[start + 5:start + 7], 'big') or 65536
    if not pointers_end <= area <= usable:
        raise PageUnavailable('invalid SQLite cell content boundary')
    cells = []
    children = []
    extents = []
    keys = []
    for i in range(count):
        index = start + header_size + i * 2
        offset = int.from_bytes(data[index:index + 2], 'big')
        if not area <= offset < usable:
            raise PageUnavailable('invalid SQLite cell pointer')
        beginning = offset
        if kind == 5:
            if offset + 4 > usable:
                raise PageUnavailable('truncated SQLite child pointer')
            children.append(int.from_bytes(data[offset:offset + 4], 'big'))
            key, offset = _varint(data, offset + 4, usable)
        else:
            payload, offset = _varint(data, offset, usable)
            key, offset = _varint(data, offset, usable)
            if payload > page_count * (usable - 4):
                raise PageUnavailable('SQLite payload exceeds image capacity')
            maximum = usable - 35
            minimum = ((usable - 12) * 32 // 255) - 23
            local = payload
            if payload > maximum:
                surplus = minimum + (payload - minimum) % (usable - 4)
                local = surplus if surplus <= maximum else minimum
            end = offset + local + (4 if local < payload else 0)
            if end > usable:
                raise PageUnavailable('truncated SQLite cell payload')
            overflow = int.from_bytes(data[offset + local:end], 'big') if local < payload else None
            if overflow is not None and not 1 < overflow <= page_count:
                raise PageUnavailable('invalid SQLite overflow pointer')
            cells.append(Cell(_row_id(key), payload, local, overflow))
            offset = end
        keys.append(_row_id(key))
        extents.append((beginning, offset))
    if any(a >= b for a, b in pairwise(keys)):
        raise PageUnavailable('SQLite row keys are not strictly ordered')
    previous_end = pointers_end
    for lo, hi in sorted(extents):
        if lo < previous_end:
            raise PageUnavailable('overlapping SQLite cells')
        previous_end = hi
    if kind == 5:
        children.append(struct.unpack('>I', data[start + 8:start + 12])[0])
        if (len(children) != len(set(children)) or page in children
                or any(not 1 < child <= page_count for child in children)):
            raise PageUnavailable('invalid SQLite B-tree child reference')
    return TreePage(page, tuple(children), tuple(cells), tuple(keys) if kind == 5 else ())



class SQLitePages:
    def __init__(self, path: Path):
        self.path = path
        self._file = path.open('rb')
        self.pages_read = 0
        self.bytes_read = 0
        try:
            self._stat = os.fstat(self._file.fileno())
            header = self._file.read(100)
            self.bytes_read += len(header)
            if len(header) != 100 or header[:16] != b'SQLite format 3\0':
                raise PageUnavailable('not a SQLite page image')
            size = int.from_bytes(header[16:18], 'big')
            self.page_size = 65536 if size == 1 else size
            if self.page_size < 512 or self.page_size > 65536 or self.page_size & (self.page_size - 1):
                raise PageUnavailable('invalid SQLite page size')
            self.usable = self.page_size - header[20]
            if self.usable < 480 or header[21:24] != bytes([64, 32, 32]):
                raise PageUnavailable('unsupported SQLite payload layout')
            if self._stat.st_size % self.page_size:
                raise PageUnavailable('truncated SQLite image')
            self.page_count = self._stat.st_size // self.page_size
            header_pages = int.from_bytes(header[28:32], 'big')
            if header[24:28] == header[92:96] and header_pages and header_pages != self.page_count:
                raise PageUnavailable('SQLite image page count mismatch')
            self._check_sidecars()
        except BaseException:
            self._file.close()
            raise

    def _check_sidecars(self) -> None:
        for suffix in ('-wal', '-journal'):
            sidecar = self.path.with_name(self.path.name + suffix)
            try:
                size = sidecar.stat().st_size
            except FileNotFoundError:
                continue
            if size:
                raise PageUnavailable('page image has unapplied SQLite sidecar data')

    def read(self, page: int) -> bytes:
        if not 1 <= page <= self.page_count:
            raise PageUnavailable('SQLite page reference outside image')
        self._file.seek((page - 1) * self.page_size)
        data = self._file.read(self.page_size)
        self.pages_read += 1
        self.bytes_read += len(data)
        if len(data) != self.page_size:
            raise PageUnavailable('SQLite image changed during read')
        return data

    def tree_page(self, page: int) -> TreePage:
        return decode_tree_page(self.read(page), page, usable=self.usable, page_count=self.page_count)

    def overflow_pages(self, cell: Cell, leaf: int) -> Iterator[int]:
        remaining = cell.payload_size - cell.local_size
        page = cell.overflow
        seen = {leaf}
        while remaining:
            if page is None or page in seen or not 1 < page <= self.page_count:
                raise PageUnavailable('invalid or cyclic SQLite overflow chain')
            seen.add(page)
            data = self.read(page)
            yield page
            remaining -= min(remaining, self.usable - 4)
            next_page = int.from_bytes(data[:4], 'big')
            if not remaining and next_page:
                raise PageUnavailable('SQLite overflow chain exceeds payload length')
            page = next_page or None
        if page is not None:
            raise PageUnavailable('unexpected SQLite overflow chain')

    def tree_pages(self, root: int) -> Iterator[TreePage]:
        pending: list[tuple[int, int | None, int | None]] = [(root, None, None)]
        seen: set[int] = set()
        while pending:
            number, lower, upper = pending.pop()
            if number in seen:
                raise PageUnavailable('cyclic or shared SQLite B-tree ownership')
            seen.add(number)
            page = self.tree_page(number)
            keys = page.separators if page.children else tuple(cell.row_id for cell in page.cells)
            if ((lower is not None and any(key <= lower for key in keys))
                    or (upper is not None and any(key > upper for key in keys))):
                raise PageUnavailable('SQLite row key outside parent bounds')
            bounds = (lower, *page.separators, upper)
            pending.extend(reversed([(child, bounds[i], bounds[i + 1]) for i, child in enumerate(page.children)]))
            yield page
        self.check_stable()

    def rows(self, root: int) -> Iterator[RowLocation]:
        seen: set[int] = set()
        for page in self.tree_pages(root):
            if page.number in seen:
                raise PageUnavailable('shared SQLite page ownership')
            seen.add(page.number)
            for cell in page.cells:
                overflow = tuple(self.overflow_pages(cell, page.number))
                if seen.intersection(overflow):
                    raise PageUnavailable('shared SQLite overflow ownership')
                seen.update(overflow)
                yield RowLocation(page.number, cell.row_id, overflow)

    def check_stable(self) -> None:
        current = os.fstat(self._file.fileno())
        named = self.path.stat()
        fields = ('st_dev', 'st_ino', 'st_size', 'st_mtime_ns', 'st_ctime_ns')
        if (any(getattr(current, field) != getattr(self._stat, field) for field in fields)
                or (named.st_dev, named.st_ino) != (current.st_dev, current.st_ino)):
            raise PageUnavailable('SQLite page image changed')
        self._check_sidecars()

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
