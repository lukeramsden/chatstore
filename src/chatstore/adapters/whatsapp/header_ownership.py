"""Transactional ownership of canonical header keys at a native capture point."""

from __future__ import annotations

import sqlite3

from ..base import IncompatibleSource
from ..native_mirror import NativeMirror


def rowid_alias(conn: sqlite3.Connection, table: str) -> str:
    row = conn.execute("SELECT rowid_alias FROM cs_native_mirror_tables WHERE name=?", (table,)).fetchone()
    if row is None or row[0] not in ('_rowid_', 'rowid', 'oid'):
        raise IncompatibleSource('native header row identity is unavailable')
    return '"' + row[0] + '"'


def prepare(conn: sqlite3.Connection, *, table: str, prefix: str, fields: str, key, full: bool) -> None:
    if not conn.in_transaction:
        raise IncompatibleSource('header ownership requires the native writer transaction')
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()[0]
    identity = rowid_alias(conn, table)
    parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
    conn.execute(f'CREATE TABLE IF NOT EXISTS {prefix}_keys(row_id INTEGER PRIMARY KEY,event_key TEXT NOT NULL)')
    conn.execute(f'CREATE INDEX IF NOT EXISTS {prefix}_key ON {prefix}_keys(event_key)')
    conn.execute(f'CREATE TABLE IF NOT EXISTS {prefix}_state(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,full INTEGER NOT NULL)')
    conn.execute(f'CREATE TABLE IF NOT EXISTS {prefix}_changes(event_key TEXT PRIMARY KEY)')
    previous = conn.execute(f'SELECT point FROM {prefix}_state WHERE id=1').fetchone()
    full = full or parent is None or previous is None or previous[0] != parent[0]
    conn.execute(f'DELETE FROM {prefix}_changes')
    if full:
        conn.execute(f'DELETE FROM {prefix}_keys')
        rows = conn.execute(f'SELECT {identity} AS cs_rowid,{fields} FROM {table}')
        conn.executemany(f'INSERT INTO {prefix}_keys VALUES(?,?)', ((row['cs_rowid'], key(row)) for row in rows))
    else:
        for change in conn.execute("SELECT row_id FROM cs_native_mirror_changes WHERE name=?", (table,)):
            old = conn.execute(f'SELECT event_key FROM {prefix}_keys WHERE row_id=?', (change[0],)).fetchone()
            if old is not None:
                conn.execute(f'INSERT OR IGNORE INTO {prefix}_changes VALUES(?)', (old[0],))
            conn.execute(f'DELETE FROM {prefix}_keys WHERE row_id=?', (change[0],))
            row = conn.execute(f'SELECT {identity} AS cs_rowid,{fields} FROM {table} WHERE {identity}=?', (change[0],)).fetchone()
            if row is not None:
                event_key = key(row)
                conn.execute(f'INSERT INTO {prefix}_keys VALUES(?,?)', (change[0], event_key))
                conn.execute(f'INSERT OR IGNORE INTO {prefix}_changes VALUES(?)', (event_key,))
    conn.execute(f'INSERT OR REPLACE INTO {prefix}_state VALUES(1,?,?)', (point, int(full)))


def selection(mirror: NativeMirror, token: str, *, prefix: str) -> tuple[set[int], set[str]]:
    with mirror.index.snapshot() as conn:
        state = conn.execute(f'SELECT point,full FROM {prefix}_state WHERE id=1').fetchone()
        native = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        if state is None or native is None or state[0] != token or native[0] != token or state[1]:
            raise IncompatibleSource('header ownership requires the exact selective native point')
        keys = {row[0] for row in conn.execute(f'SELECT event_key FROM {prefix}_changes')}
        rows = {row[0] for row in conn.execute(f'''SELECT row_id FROM {prefix}_changes changed
            CROSS JOIN {prefix}_keys owned INDEXED BY {prefix}_key
            WHERE owned.event_key=changed.event_key''')}
        return rows, keys
