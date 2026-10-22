"""Point-bound event-key ownership for selective group context extraction."""

from __future__ import annotations

import sqlite3

from ...identity import urn as U
from ..base import IncompatibleSource
from ..native_mirror import NativeMirror


def key(row) -> str:
    return U.composite_key('groupchange', U.whatsapp_jid(row['ZGROUPJID'] or ''),
                           str(row['ZCHANGEDATE']), str(row['ZCHANGETYPE']))


def rowid_alias(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT rowid_alias FROM cs_native_mirror_tables WHERE name='ZWAGROUPMEMBERSCHANGE'").fetchone()
    if row is None or row[0] not in ('_rowid_', 'rowid', 'oid'):
        raise IncompatibleSource('group event native row identity is unavailable')
    return '"' + row[0] + '"'


def prepare(conn: sqlite3.Connection, *, full: bool) -> None:
    if not conn.in_transaction:
        raise IncompatibleSource('event ownership requires the native writer transaction')
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()[0]
    identity = rowid_alias(conn)
    parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
    conn.execute('CREATE TABLE IF NOT EXISTS cs_wa_event_keys(row_id INTEGER PRIMARY KEY,event_key TEXT NOT NULL)')
    conn.execute('CREATE INDEX IF NOT EXISTS cs_wa_event_key ON cs_wa_event_keys(event_key)')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_wa_event_state(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,full INTEGER NOT NULL)')
    conn.execute('CREATE TABLE IF NOT EXISTS cs_wa_event_changes(event_key TEXT PRIMARY KEY)')
    previous = conn.execute('SELECT point FROM cs_wa_event_state WHERE id=1').fetchone()
    full = full or parent is None or previous is None or previous[0] != parent[0]
    conn.execute('DELETE FROM cs_wa_event_changes')
    if full:
        conn.execute('DELETE FROM cs_wa_event_keys')
        rows = conn.execute(f'SELECT {identity} AS cs_rowid,ZGROUPJID,ZCHANGEDATE,ZCHANGETYPE FROM ZWAGROUPMEMBERSCHANGE')
        conn.executemany('INSERT INTO cs_wa_event_keys VALUES(?,?)', ((row['cs_rowid'], key(row)) for row in rows))
    else:
        for change in conn.execute("SELECT row_id FROM cs_native_mirror_changes WHERE name='ZWAGROUPMEMBERSCHANGE'"):
            old = conn.execute('SELECT event_key FROM cs_wa_event_keys WHERE row_id=?', (change[0],)).fetchone()
            if old is not None:
                conn.execute('INSERT OR IGNORE INTO cs_wa_event_changes VALUES(?)', (old[0],))
            conn.execute('DELETE FROM cs_wa_event_keys WHERE row_id=?', (change[0],))
            row = conn.execute(f'SELECT {identity} AS cs_rowid,ZGROUPJID,ZCHANGEDATE,ZCHANGETYPE FROM ZWAGROUPMEMBERSCHANGE WHERE {identity}=?', (change[0],)).fetchone()
            if row is not None:
                event_key = key(row)
                conn.execute('INSERT INTO cs_wa_event_keys VALUES(?,?)', (change[0], event_key))
                conn.execute('INSERT OR IGNORE INTO cs_wa_event_changes VALUES(?)', (event_key,))
    conn.execute('INSERT OR REPLACE INTO cs_wa_event_state VALUES(1,?,?)', (point, int(full)))


def selection(mirror: NativeMirror, token: str) -> tuple[set[int], set[str]]:
    with mirror.index.snapshot() as conn:
        state = conn.execute('SELECT point,full FROM cs_wa_event_state WHERE id=1').fetchone()
        native = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
        if state is None or native is None or state[0] != token or native[0] != token or state[1]:
            raise IncompatibleSource('event ownership requires the exact selective native point')
        keys = {row[0] for row in conn.execute('SELECT event_key FROM cs_wa_event_changes')}
        rows = {row[0] for row in conn.execute('''SELECT row_id FROM cs_wa_event_changes changed
            CROSS JOIN cs_wa_event_keys owned INDEXED BY cs_wa_event_key
            WHERE owned.event_key=changed.event_key''')}
        return rows, keys
