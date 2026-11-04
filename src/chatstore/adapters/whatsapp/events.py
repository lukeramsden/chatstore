"""Group membership-change event keys and their point-bound selective ownership.

The canonical key of a `ZWAGROUPMEMBERSCHANGE` row is `key(row)` (group, change
date, change type). Distinct rows can share it, so `keyed()` resolves each
collision group as a whole: the lowest-`Z_PK` row keeps the base key and every
other distinct member list gets `disambiguated(base, members)`. Ownership and
selection therefore work in base keys; selected rows are always whole groups.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable

from ...identity import urn as U
from ..native_mirror import NativeMirror
from . import header_ownership

POLICY_MODULES = (header_ownership,)
TABLE = 'ZWAGROUPMEMBERSCHANGE'


def key(row) -> str:
    return U.composite_key('groupchange', U.whatsapp_jid(row['ZGROUPJID'] or ''),
                           str(row['ZCHANGEDATE']), str(row['ZCHANGETYPE']))


def members(row) -> list[str]:
    return [U.whatsapp_jid(m) for m in str(row['ZMEMBERJIDS'] or '').replace(';', ',').split(',') if m.strip()]


def disambiguated(base: str, member_jids: list[str]) -> str:
    # A strict extension of the base array: `base[:-1] + ','` prefixes every variant.
    return base[:-1] + ',' + U.composite_key(','.join(member_jids))[1:]


def keyed(rows: Iterable) -> list[tuple[object, str, list[str]]]:
    """(row, canonical key, members) in input order; input must hold whole base-key groups."""
    rows = [(row, key(row), members(row)) for row in rows]
    holders: dict[str, tuple[tuple[bool, int], list[str]]] = {}
    for row, base, jids in rows:
        rank = (row['Z_PK'] is None, row['Z_PK'] or 0)
        if base not in holders or rank < holders[base][0]:
            holders[base] = (rank, jids)
    return [(row, base if jids == holders[base][1] else disambiguated(base, jids), jids)
            for row, base, jids in rows]


def observed_keys(conn: sqlite3.Connection, source: str, scope: str, keys: Iterable[str]) -> set[str]:
    """Base keys plus every disambiguated variant already observed for them."""
    found = set(keys)
    for base in sorted(found):
        stem = base[:-1]
        found.update(row[0] for row in conn.execute('''SELECT native_key FROM source_observations
            INDEXED BY observations_native WHERE source=? AND account_scope=? AND native_table=?
            AND native_key>? AND native_key<?''', (source, scope, TABLE, stem + ',', stem + '-')))
    return found


def rowid_alias(conn: sqlite3.Connection) -> str:
    return header_ownership.rowid_alias(conn, TABLE)


def prepare(conn: sqlite3.Connection, *, full: bool) -> None:
    header_ownership.prepare(conn, table=TABLE, prefix='cs_wa_event',
        fields='ZGROUPJID,ZCHANGEDATE,ZCHANGETYPE', key=key, full=full)


def selection(mirror: NativeMirror, token: str) -> tuple[set[int], set[str]]:
    return header_ownership.selection(mirror, token, prefix='cs_wa_event')
