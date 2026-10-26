"""Point-bound event-key ownership for selective group context extraction."""

from __future__ import annotations

import sqlite3

from ...identity import urn as U
from ..native_mirror import NativeMirror
from . import header_ownership

POLICY_MODULES = (header_ownership,)


def key(row) -> str:
    return U.composite_key('groupchange', U.whatsapp_jid(row['ZGROUPJID'] or ''),
                           str(row['ZCHANGEDATE']), str(row['ZCHANGETYPE']))


def rowid_alias(conn: sqlite3.Connection) -> str:
    return header_ownership.rowid_alias(conn, 'ZWAGROUPMEMBERSCHANGE')


def prepare(conn: sqlite3.Connection, *, full: bool) -> None:
    header_ownership.prepare(conn, table='ZWAGROUPMEMBERSCHANGE', prefix='cs_wa_event',
        fields='ZGROUPJID,ZCHANGEDATE,ZCHANGETYPE', key=key, full=full)


def selection(mirror: NativeMirror, token: str) -> tuple[set[int], set[str]]:
    return header_ownership.selection(mirror, token, prefix='cs_wa_event')
