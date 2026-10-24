"""Transactional row/overflow ownership for private SQLite page images.

Updates require schema-compatible images and a complete changed-page set for two
coordinated source points. The caller supplies a source schema/policy tag.
The opaque point tag binds this projection to its caller's baseline; publishing a
native capture cursor remains the caller's responsibility.
"""

from __future__ import annotations

import json
import os
import sqlite3
from collections import defaultdict
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Self

from .pages import Cell, PageUnavailable, TreePage


class PageImage(Protocol):
    page_size: int
    usable: int
    page_count: int

    def tree_page(self, page: int) -> TreePage: ...
    def overflow_pages(self, cell: Cell, leaf: int) -> Iterable[int]: ...
    def check_stable(self) -> None: ...


@dataclass(frozen=True)
class RowCandidates:
    rows: dict[str, frozenset[int]]
    tree_pages_read: int


class PageIndex:
    def __init__(self, path: Path):
        self.path = path
        fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600)
        os.close(fd)
        self.conn = sqlite3.connect(path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        try:
            self.conn.execute('PRAGMA journal_mode=WAL')
            self.conn.execute('PRAGMA synchronous=FULL')
            self.conn.execute('PRAGMA cache_size=-65536')
            for statement in (
                'CREATE TABLE IF NOT EXISTS cs_native_page_state(id INTEGER PRIMARY KEY CHECK(id=1), point TEXT NOT NULL, page_size INTEGER NOT NULL, usable INTEGER NOT NULL, schema_tag TEXT NOT NULL)',
                'CREATE TABLE IF NOT EXISTS cs_native_page_roots(name TEXT PRIMARY KEY,page INTEGER NOT NULL UNIQUE)',
                '''CREATE TABLE IF NOT EXISTS cs_native_page_nodes(page INTEGER PRIMARY KEY, name TEXT NOT NULL,
                    minimum INTEGER, maximum INTEGER, children TEXT NOT NULL, rows TEXT NOT NULL, cells TEXT NOT NULL)''',
                '''CREATE TABLE IF NOT EXISTS cs_native_page_edges(child INTEGER PRIMARY KEY,parent INTEGER NOT NULL,
                    position INTEGER NOT NULL,lower_key INTEGER,upper_key INTEGER)''',
                'CREATE INDEX IF NOT EXISTS cs_native_page_edge_parent ON cs_native_page_edges(parent)',
                'CREATE TABLE IF NOT EXISTS cs_native_page_overflow(page INTEGER PRIMARY KEY,leaf INTEGER NOT NULL,row_id INTEGER NOT NULL,next_page INTEGER NOT NULL)',
                'CREATE INDEX IF NOT EXISTS cs_native_page_overflow_leaf ON cs_native_page_overflow(leaf)',
            ):
                self.conn.execute(statement)

            formats = {
                'cs_native_page_state': ['id', 'point', 'page_size', 'usable', 'schema_tag'],
                'cs_native_page_nodes': ['page', 'name', 'minimum', 'maximum', 'children', 'rows', 'cells'],
                'cs_native_page_overflow': ['page', 'leaf', 'row_id', 'next_page'],
            }
            for table, fields in formats.items():
                if [row[1] for row in self.conn.execute(f'PRAGMA table_info({table})')] != fields:
                    raise PageUnavailable('unsupported row-page index format; rebuild required')
        except BaseException:
            self.conn.close()
            raise

    def build(self, image: PageImage, roots: Mapping[str, int], *, point: str, schema_tag: str) -> RowCandidates:
        return self._apply(image, roots, None, None, point, schema_tag)

    def update(self, image: PageImage, roots: Mapping[str, int], changed: Iterable[int], *,
               expected_point: str, point: str, schema_tag: str) -> RowCandidates:
        return self._apply(image, roots, set(changed), expected_point, point, schema_tag)

    def _apply(self, image: PageImage, roots: Mapping[str, int], changed: set[int] | None,
               expected_point: str | None, point: str, schema_tag: str) -> RowCandidates:
        db = self.conn
        nested = db.in_transaction
        db.execute('SAVEPOINT cs_native_page_update' if nested else 'BEGIN IMMEDIATE')
        try:
            state = db.execute('SELECT point,page_size,usable,schema_tag FROM cs_native_page_state WHERE id=1').fetchone()
            if changed is not None and (state is None or state['point'] != expected_point
                    or state['page_size'] != image.page_size or state['usable'] != image.usable or state['schema_tag'] != schema_tag or dict(db.execute('SELECT name,page FROM cs_native_page_roots')) != dict(roots)):
                raise PageUnavailable('row-page index baseline does not match capture point or roots')
            if changed is None:
                for table in ('cs_native_page_overflow', 'cs_native_page_edges', 'cs_native_page_nodes', 'cs_native_page_roots', 'cs_native_page_state'):
                    db.execute(f'DELETE FROM {table}')
                db.executemany('INSERT INTO cs_native_page_roots VALUES (?,?)', roots.items())
            root_pages = set(roots.values())
            candidates: dict[str, set[int]] = defaultdict(set)
            needed: set[int] = set()
            orphaned: set[int] = set()
            visiting: set[int] = set()
            visited: set[int] = set()
            read_count = 0

            def remember(node):
                candidates[node['name']].update(json.loads(node['rows']))

            def cut_edges(page):
                orphaned.update(row[0] for row in db.execute('SELECT child FROM cs_native_page_edges WHERE parent=?', (page,)))
                db.execute('DELETE FROM cs_native_page_edges WHERE parent=?', (page,))

            def forget(page):
                node = db.execute('SELECT * FROM cs_native_page_nodes WHERE page=?', (page,)).fetchone()
                if node is not None:
                    remember(node)
                    cut_edges(page)
                    db.execute('DELETE FROM cs_native_page_overflow WHERE leaf=?', (page,))
                    db.execute('DELETE FROM cs_native_page_nodes WHERE page=?', (page,))

            if changed is not None:
                pending = []
                for page in changed:
                    if db.execute('SELECT 1 FROM cs_native_page_nodes WHERE page=?', (page,)).fetchone():
                        pending.append(page)
                    owner = db.execute('SELECT leaf FROM cs_native_page_overflow WHERE page=?', (page,)).fetchone()
                    if owner:
                        pending.append(owner[0])
                pending.extend(row[0] for row in db.execute('SELECT page FROM cs_native_page_nodes WHERE page>?', (image.page_count,)))
                pending.extend(row[0] for row in db.execute('SELECT DISTINCT leaf FROM cs_native_page_overflow WHERE page>?', (image.page_count,)))
                while pending:
                    page = pending.pop()
                    if page in needed:
                        continue
                    needed.add(page)
                    parent = db.execute('SELECT parent FROM cs_native_page_edges WHERE child=?', (page,)).fetchone()
                    if parent:
                        pending.append(parent[0])
                for page in needed:
                    node = db.execute('SELECT * FROM cs_native_page_nodes WHERE page=?', (page,)).fetchone()
                    if node:
                        remember(node)
                    cut_edges(page)
                    db.execute('DELETE FROM cs_native_page_overflow WHERE leaf=?', (page,))

            def link(child, parent, position, lower, upper):
                previous = db.execute('SELECT parent FROM cs_native_page_edges WHERE child=?', (child,)).fetchone()
                if previous:
                    old_parent = previous[0]
                    chain = set()
                    ancestor = old_parent
                    while ancestor not in root_pages:
                        if ancestor in chain or len(chain) >= 64:
                            raise PageUnavailable('cyclic index ancestry')
                        chain.add(ancestor)
                        edge = db.execute('SELECT parent FROM cs_native_page_edges WHERE child=?', (ancestor,)).fetchone()
                        if not edge:
                            break
                        ancestor = edge[0]
                    if ancestor in root_pages:
                        raise PageUnavailable('B-tree page has two reachable parents')
                    needed.update(chain)
                    db.execute('DELETE FROM cs_native_page_edges WHERE child=?', (child,))
                db.execute('INSERT INTO cs_native_page_edges VALUES (?,?,?,?,?)', (child, parent, position, lower, upper))

            def bounds_ok(minimum, maximum, lower, upper):
                if ((minimum is not None and lower is not None and minimum <= lower)
                        or (maximum is not None and upper is not None and maximum > upper)):
                    raise PageUnavailable('indexed rows violate SQLite parent bounds')

            def visit(page, name, lower=None, upper=None):
                nonlocal read_count
                if page in visiting or page in visited or len(visiting) >= 64:
                    raise PageUnavailable('cyclic/shared or excessively deep row-page graph')
                node = db.execute('SELECT name,minimum,maximum FROM cs_native_page_nodes WHERE page=?', (page,)).fetchone()
                if changed is not None and node is not None and page not in needed:
                    if node['name'] != name:
                        raise PageUnavailable('unchanged page moved across table ownership')
                    bounds_ok(node['minimum'], node['maximum'], lower, upper)
                    return node['minimum'], node['maximum']
                if db.execute('SELECT 1 FROM cs_native_page_overflow WHERE page=?', (page,)).fetchone():
                    raise PageUnavailable('page shared by B-tree and overflow payload')
                visiting.add(page)
                visited.add(page)
                if node:
                    remember(db.execute('SELECT name,rows FROM cs_native_page_nodes WHERE page=?', (page,)).fetchone())
                current = image.tree_page(page)
                read_count += 1
                keys = current.separators if current.children else tuple(cell.row_id for cell in current.cells)
                bounds_ok(keys[0] if keys else None, keys[-1] if keys else None, lower, upper)
                cut_edges(page)
                db.execute('DELETE FROM cs_native_page_overflow WHERE leaf=?', (page,))
                row_ids = [cell.row_id for cell in current.cells]
                db.execute('''INSERT INTO cs_native_page_nodes(page,name,children,rows,cells,minimum,maximum) VALUES (?,?,?,?,?,NULL,NULL) ON CONFLICT(page) DO UPDATE SET
                    name=excluded.name,children=excluded.children,rows=excluded.rows,cells=excluded.cells,minimum=NULL,maximum=NULL''',
                    (page, name, json.dumps(current.children), json.dumps(row_ids),
                     json.dumps([[cell.row_id, cell.payload_size, cell.local_size, cell.overflow] for cell in current.cells])))
                candidates[name].update(row_ids)
                minima, maxima = [], []
                if current.children:
                    bounds = (lower, *current.separators, upper)
                    for position, child in enumerate(current.children):
                        if child in root_pages:
                            raise PageUnavailable('table root reused as a child page')
                        link(child, page, position, bounds[position], bounds[position + 1])
                        lo, hi = visit(child, name, bounds[position], bounds[position + 1])
                        if lo is not None:
                            minima.append(lo)
                            maxima.append(hi)
                else:
                    minima, maxima = row_ids, row_ids
                    for cell in current.cells:
                        previous_overflow = None
                        for overflow in image.overflow_pages(cell, page):
                            if db.execute('SELECT 1 FROM cs_native_page_nodes WHERE page=?', (overflow,)).fetchone():
                                if (overflow in root_pages or overflow in visited or db.execute(
                                        'SELECT 1 FROM cs_native_page_edges WHERE child=?', (overflow,)).fetchone()):
                                    raise PageUnavailable('overflow payload reuses a reachable B-tree page')
                                forget(overflow)
                            db.execute('INSERT INTO cs_native_page_overflow VALUES (?,?,?,0)', (overflow, page, cell.row_id))
                            if previous_overflow is not None:
                                db.execute('UPDATE cs_native_page_overflow SET next_page=? WHERE page=?', (overflow, previous_overflow))
                            previous_overflow = overflow
                minimum = min(minima) if minima else None
                maximum = max(maxima) if maxima else None
                bounds_ok(minimum, maximum, lower, upper)
                db.execute('UPDATE cs_native_page_nodes SET minimum=?,maximum=? WHERE page=?', (minimum, maximum, page))
                visiting.remove(page)
                return minimum, maximum

            for name, root in sorted(roots.items()):
                if changed is None or root in needed:
                    visit(root, name)
            while orphaned:
                page = orphaned.pop()
                if page not in root_pages and not db.execute('SELECT 1 FROM cs_native_page_edges WHERE child=?', (page,)).fetchone():
                    forget(page)
            image.check_stable()
            db.execute('INSERT INTO cs_native_page_state VALUES (1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET point=excluded.point,page_size=excluded.page_size,usable=excluded.usable,schema_tag=excluded.schema_tag',
                       (point, image.page_size, image.usable, schema_tag))
            db.execute('RELEASE cs_native_page_update' if nested else 'COMMIT')
            return RowCandidates({name: frozenset(rows) for name, rows in candidates.items() if rows}, read_count)
        except sqlite3.IntegrityError as exc:
            if nested:
                db.execute('ROLLBACK TO cs_native_page_update')
                db.execute('RELEASE cs_native_page_update')
            else:
                db.execute('ROLLBACK')
            raise PageUnavailable('conflicting row-page ownership') from exc
        except BaseException:
            if nested:
                db.execute('ROLLBACK TO cs_native_page_update')
                db.execute('RELEASE cs_native_page_update')
            else:
                db.execute('ROLLBACK')
            raise

    @contextmanager
    def snapshot(self):
        reader = sqlite3.connect(self.path.absolute().as_uri() + '?mode=ro', uri=True, isolation_level=None)
        reader.row_factory = sqlite3.Row
        try:
            reader.execute('PRAGMA query_only=1')
            reader.execute('BEGIN')
            reader.execute('SELECT point FROM cs_native_page_state').fetchone()
            yield reader
        finally:
            reader.close()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
