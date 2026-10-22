"""Frozen WhatsApp alias evidence from pinned optional database views."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from typing import Any

from ...identity import urn as U
from ..base import IncompatibleSource

SOURCES = {
    'LID': ('ZWAZACCOUNT', 'ZIDENTIFIER', 'ZPHONENUMBER'),
    'ContactsV2': ('ZWAADDRESSBOOKCONTACT', 'ZLID', 'ZWHATSAPPID'),
}


def collect(sources: Mapping[str, sqlite3.Connection | None]) -> dict[str, list[list[Any]] | None]:
    if set(sources) != set(SOURCES):
        raise IncompatibleSource('alias capture requires explicit presence for both optional databases')
    result: dict[str, list[list[Any]] | None] = {}
    for name, (table, left, right) in SOURCES.items():
        source = sources[name]
        if source is None:
            result[name] = None
        elif not source.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (table,)).fetchone():
            result[name] = []
        else:
            result[name] = [list(row) for row in source.execute(
                f'SELECT {left},{right} FROM {table} WHERE {left} IS NOT NULL AND {right} IS NOT NULL')]
    return result


def pairs(rows: Mapping[str, list[list[Any]] | None]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    result: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for name in SOURCES:
        for lid, phone in rows[name] or []:
            if name == 'LID':
                digits = ''.join(ch for ch in str(phone) if ch.isdigit())
                if not digits or '@lid' not in lid:
                    continue
                key = (U.whatsapp_jid(lid), f'{digits}@s.whatsapp.net')
                evidence = {'type': 'lid_phone_pair', 'detail': 'LID.sqlite/ZWAZACCOUNT'}
            else:
                key = (U.whatsapp_jid(lid), U.whatsapp_jid(phone))
                evidence = {'type': 'address_book', 'detail': 'ContactsV2.sqlite/ZWAADDRESSBOOKCONTACT'}
            result.setdefault(key, []).append(evidence)
    return result


def prepare(conn: sqlite3.Connection, *, sources: Mapping[str, sqlite3.Connection | None]) -> None:
    if not conn.in_transaction or any(source is not None and not source.in_transaction for source in sources.values()):
        raise IncompatibleSource('alias preparation requires native writer and pinned source transactions')
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
    if point is None:
        raise IncompatibleSource('alias preparation requires a candidate page point')
    rows = collect(sources)
    pairs(rows)  # Validate native values before publishing the retained point.
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_wa_aliases(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,format INTEGER NOT NULL,payload TEXT NOT NULL)')
    conn.execute('INSERT INTO cs_native_wa_aliases VALUES(1,?,1,?) ON CONFLICT(id) DO UPDATE SET point=excluded.point,format=1,payload=excluded.payload',
                 (point[0], json.dumps(rows, sort_keys=True, allow_nan=False)))
    conn.execute('CREATE TABLE IF NOT EXISTS cs_native_wa_alias_state(id INTEGER PRIMARY KEY CHECK(id=1),point TEXT NOT NULL,evidence_point TEXT NOT NULL)')
    conn.execute('INSERT INTO cs_native_wa_alias_state VALUES(1,?,?) ON CONFLICT(id) DO UPDATE SET point=excluded.point,evidence_point=excluded.evidence_point',
                 (point[0], point[0]))


def reuse(conn: sqlite3.Connection, parent: str) -> bool:
    """Rebind unchanged evidence without reading or rewriting its alias payload."""
    if not conn.in_transaction:
        raise IncompatibleSource('alias reuse requires the native writer transaction')
    if not conn.execute("SELECT 1 FROM sqlite_schema WHERE name='cs_native_wa_alias_state' AND type='table'").fetchone():
        return False
    point = conn.execute('SELECT point FROM cs_native_page_state WHERE id=1').fetchone()
    if point is None:
        raise IncompatibleSource('alias reuse requires a candidate page point')
    return conn.execute('''UPDATE cs_native_wa_alias_state SET point=? WHERE id=1 AND point=?
        AND EXISTS(SELECT 1 FROM cs_native_wa_aliases aliases WHERE aliases.id=1
            AND aliases.point=cs_native_wa_alias_state.evidence_point AND aliases.format=1)''',
        (point[0], parent)).rowcount == 1


def read(conn: sqlite3.Connection, token: str) -> dict[tuple[str, str], list[dict[str, Any]]]:
    row = conn.execute('''SELECT aliases.payload FROM cs_native_wa_alias_state state
        CROSS JOIN cs_native_wa_aliases aliases CROSS JOIN cs_native_mirror_state native
        WHERE aliases.id=1 AND state.id=1 AND native.id=1 AND state.point=?
        AND state.evidence_point=aliases.point AND native.token=? AND aliases.format=1''', (token, token)).fetchone()
    if row is None:
        raise IncompatibleSource('prepared aliases have no matching native point')
    return pairs(json.loads(row[0]))
