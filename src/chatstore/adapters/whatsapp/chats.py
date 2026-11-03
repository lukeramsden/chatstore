"""Selective chat metadata when identity and membership inputs are unchanged."""

from __future__ import annotations

import json
import sqlite3

from ...identity import urn as U
from ..base import IncompatibleSource
from ..native_mirror import NativeMirror, PreparedCapture
from . import header_ownership

POLICY_MODULES = (header_ownership,)


def key(row) -> str:
    return U.whatsapp_jid(row['ZCONTACTJID'])


def prepare(conn: sqlite3.Connection, *, full: bool) -> None:
    header_ownership.prepare(conn, table='ZWACHATSESSION', prefix='cs_wa_chat',
                             fields='ZCONTACTJID', key=key, full=full)


def selection(mirror: NativeMirror, capture: PreparedCapture) -> tuple[set[int], set[str]]:
    from .adapter import REQUIRED, chat_kind

    # WhatsApp rewrites its chat row on every message (last message, dates, counters).
    # Only columns the adapter reads can change canonical output; the rest are ignored.
    allowed = {'ZPARTNERNAME', 'ZARCHIVED', 'ZREMOVED', 'ZHIDDEN'}
    inputs = REQUIRED['ZWACHATSESSION'] - allowed
    with mirror.index.snapshot() as reader:
        if mirror.pending(reader=reader) != capture:
            raise IncompatibleSource('chat metadata requires the exact pending capture')
        columns = json.loads(reader.execute("SELECT columns FROM cs_native_mirror_tables WHERE name='ZWACHATSESSION'").fetchone()[0])
        for change in mirror.changes(capture.token, reader=reader, tables=['ZWACHATSESSION']):
            if change.before is None or change.after is None:
                raise IncompatibleSource('chat insertion or deletion changes identity coverage')
            before = dict(zip(columns, change.before, strict=True))
            after = dict(zip(columns, change.after, strict=True))
            if any(type(before[field]) is not type(after[field]) or before[field] != after[field]
                   for field in inputs):
                raise IncompatibleSource('chat identity or membership inputs changed')
            if before['ZPARTNERNAME'] != after['ZPARTNERNAME'] and chat_kind(key(before), before['ZSESSIONTYPE']) != 'group':
                raise IncompatibleSource('direct chat name contributes to identity evidence')
    return header_ownership.selection(mirror, capture.token, prefix='cs_wa_chat')
