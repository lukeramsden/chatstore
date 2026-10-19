"""Changed SQLite pages plus a pinned old ownership graph; no new full image.

The caller must keep the matching source read view and index snapshot open until
PageIndex.update succeeds. This selects candidates only, not durable source capture.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator

from .pages import Cell, PageUnavailable, TreePage, decode_tree_page
from .source_view import SourceReadView
from .wal import WalPages


class WalPageImage:
    def __init__(self, view: SourceReadView, pages: WalPages, metadata: sqlite3.Connection, *,
                 expected_point: str, schema_tag: str):
        self.view = view
        self.pages = pages
        self.metadata = metadata
        self.expected_point = expected_point
        self.schema_tag = schema_tag
        if view.baseline_required or view.changes != pages.changes:
            raise PageUnavailable('WAL page image requires a matching continuation view')
        state = metadata.execute('SELECT point,page_size,usable,schema_tag FROM cs_native_page_state WHERE id=1').fetchone()
        if state is None or (state['point'], state['schema_tag']) != (expected_point, schema_tag):
            raise PageUnavailable('WAL page image baseline or schema tag mismatch')
        self.page_size = view.changes.cursor.page_size
        if state['page_size'] != self.page_size or view.changes.cursor.database_pages is None:
            raise PageUnavailable('WAL page image geometry mismatch')
        self.usable = state['usable']
        self.page_count = view.changes.cursor.database_pages
        if 1 in view.changes.pages:
            header = pages.read(1)
            size = int.from_bytes(header[16:18], 'big')
            size = 65536 if size == 1 else size
            if (header[:16] != b'SQLite format 3\0' or size != self.page_size
                    or size - header[20] != self.usable or header[21:24] != bytes([64, 32, 32])
                    or int.from_bytes(header[40:44], 'big') != view.schema_version):
                raise PageUnavailable('changed SQLite header requires reconciliation')
        self.check_stable()

    def tree_page(self, page: int) -> TreePage:
        if not 1 <= page <= self.page_count:
            raise PageUnavailable('WAL tree page reference outside image')
        if page in self.view.changes.pages:
            return decode_tree_page(self.pages.read(page), page, usable=self.usable, page_count=self.page_count)
        node = self.metadata.execute('SELECT children,cells FROM cs_native_page_nodes WHERE page=?', (page,)).fetchone()
        if node is None:
            raise PageUnavailable('unchanged tree page absent from baseline')
        children = tuple(json.loads(node['children']))
        cells = tuple(Cell(*fields) for fields in json.loads(node['cells']))
        edges = self.metadata.execute('SELECT child,position,upper_key FROM cs_native_page_edges WHERE parent=? ORDER BY position', (page,)).fetchall()
        if (tuple(row['child'] for row in edges) != children
                or tuple(row['position'] for row in edges) != tuple(range(len(children)))
                or any(row['upper_key'] is None for row in edges[:-1])):
            raise PageUnavailable('baseline tree edges are inconsistent')
        return TreePage(page, children, cells, tuple(row['upper_key'] for row in edges[:-1]))

    def overflow_pages(self, cell: Cell, leaf: int) -> Iterator[int]:
        remaining = cell.payload_size - cell.local_size
        page = cell.overflow
        seen = {leaf}
        while remaining:
            if page is None or page in seen or not 1 < page <= self.page_count:
                raise PageUnavailable('invalid or cyclic WAL overflow chain')
            seen.add(page)
            if page in self.view.changes.pages:
                next_page = int.from_bytes(self.pages.read(page)[:4], 'big')
            else:
                old = self.metadata.execute('SELECT next_page FROM cs_native_page_overflow WHERE page=?', (page,)).fetchone()
                if old is None:
                    raise PageUnavailable('unchanged overflow page absent from baseline')
                next_page = old['next_page']
            yield page
            remaining -= min(remaining, self.usable - 4)
            if not remaining and next_page:
                raise PageUnavailable('WAL overflow chain exceeds payload length')
            page = next_page or None
        if page is not None:
            raise PageUnavailable('unexpected WAL overflow chain')

    def check_stable(self) -> None:
        if not self.metadata.in_transaction or not self.view.connection.in_transaction:
            raise PageUnavailable('WAL image source or metadata snapshot is no longer pinned')
        if self.view.connection.execute('PRAGMA schema_version').fetchone()[0] != self.view.schema_version:
            raise PageUnavailable('WAL image source schema changed')
        self.pages.check_stable()
