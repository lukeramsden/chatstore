"""Point-bound header fingerprints: exactly which chat, membership, identity and alias
records a WhatsApp delta can change.

Every header record is a pure function of its `HeaderEntry` payload. Inside the
native writer transaction, after alias evidence is prepared, this hashes every
entry's payload per (native table, native key), diffs the hashes against the
parent point's and records the changed keys. A key whose payload changed, appeared
or vanished is in the change set, whatever native row or alias caused it. Canonical
capture then withdraws and re-extracts only those keys.

A missing or foreign parent state, or a full capture, records every key and marks
the state full, which forbids selective use.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from collections import defaultdict

from ..base import ExtractStats, IncompatibleSource
from ..native_mirror import NativeMirror, PreparedCapture
from . import adapter, aliases

POLICY_MODULES = (adapter, aliases)

FORMAT = 1


def fingerprints(conn: sqlite3.Connection, alias_pairs) -> dict[tuple[str, str], str]:
    grouped: dict[tuple[str, str], list[object]] = defaultdict(list)
    entries, _ = adapter.header_entries(conn, alias_pairs, ExtractStats())
    for entry in entries:
        # Duplicate keys (two native rows for one canonical key) hash together, in order.
        grouped[(entry.table, entry.key)].append((entry.kind, entry.payload))
    # Payloads hold only SQLite scalars, lists and dicts with a fixed key order, so repr
    # is deterministic; the extraction policy already pins the Python version.
    return {key: hashlib.blake2b(repr(value).encode(), digest_size=16).hexdigest() for key, value in grouped.items()}


def prepare(conn: sqlite3.Connection, *, full: bool) -> None:
    """Called as a native pre-commit validator after alias evidence is prepared."""
    if not conn.in_transaction:
        raise IncompatibleSource('header fingerprints require the native writer transaction')
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
    if point is None:
        raise IncompatibleSource('header fingerprints require a candidate page point')
    parent = conn.execute('SELECT token FROM cs_native_mirror_state WHERE id=1').fetchone()
    payload = conn.execute('SELECT payload FROM cs_native_wa_aliases WHERE id=1 AND format=1').fetchone()
    if payload is None:
        raise IncompatibleSource('header fingerprints require prepared alias evidence')
    conn.execute('''CREATE TABLE IF NOT EXISTS cs_wa_header_fp(
        native_table TEXT NOT NULL,native_key TEXT NOT NULL,hash TEXT NOT NULL,PRIMARY KEY(native_table,native_key))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS cs_wa_header_state(
        id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,format INTEGER NOT NULL,full INTEGER NOT NULL)''')
    conn.execute('''CREATE TABLE IF NOT EXISTS cs_wa_header_changes(
        native_table TEXT NOT NULL,native_key TEXT NOT NULL,PRIMARY KEY(native_table,native_key))''')
    current = fingerprints(conn, aliases.pairs(json.loads(payload[0])))
    previous = conn.execute('SELECT point,format FROM cs_wa_header_state WHERE id=1').fetchone()
    full = full or parent is None or previous is None or (previous[0], previous[1]) != (parent[0], FORMAT)
    conn.execute('DELETE FROM cs_wa_header_changes')
    if full:
        conn.execute('DELETE FROM cs_wa_header_fp')
        conn.executemany('INSERT INTO cs_wa_header_fp VALUES(?,?,?)', ((t, k, h) for (t, k), h in current.items()))
    else:
        stored = {(row[0], row[1]): row[2] for row in conn.execute('SELECT native_table,native_key,hash FROM cs_wa_header_fp')}
        changed = {key for key in stored.keys() | current.keys() if stored.get(key) != current.get(key)}
        conn.executemany('INSERT INTO cs_wa_header_changes VALUES(?,?)', sorted(changed))
        conn.executemany('DELETE FROM cs_wa_header_fp WHERE native_table=? AND native_key=?',
                         sorted(key for key in changed if key not in current))
        conn.executemany('INSERT OR REPLACE INTO cs_wa_header_fp VALUES(?,?,?)',
                         sorted((t, k, current[(t, k)]) for t, k in changed if (t, k) in current))
    conn.execute('INSERT OR REPLACE INTO cs_wa_header_state VALUES(1,?,?,?)', (point[0], FORMAT, int(full)))


def selection(mirror: NativeMirror, capture: PreparedCapture) -> dict[str, set[str]]:
    """Changed header keys by observation native table, for the exact pending point."""
    with mirror.index.snapshot() as conn:
        if mirror.pending(reader=conn) != capture:
            raise IncompatibleSource('header selection requires the exact pending capture')
        if not conn.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name='cs_wa_header_state'").fetchone():
            raise IncompatibleSource('header fingerprints are unavailable')
        state = conn.execute('SELECT point,format,full FROM cs_wa_header_state WHERE id=1').fetchone()
        if state is None or (state[0], state[1], state[2]) != (capture.token, FORMAT, 0):
            raise IncompatibleSource('header selection requires a selective fingerprint state at this point')
        keys: dict[str, set[str]] = {}
        for table, key in conn.execute('SELECT native_table,native_key FROM cs_wa_header_changes'):
            keys.setdefault(table, set()).add(key)
        return keys
