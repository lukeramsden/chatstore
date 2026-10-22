"""Indexed dependency routing for a prepared WhatsApp ChatStorage point."""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass

from ..native_mirror import NativeMirror, PreparedCapture
from .adapter import REQUIRED


class RoutingUnavailable(Exception):
    pass


@dataclass(frozen=True)
class WhatsAppSelection:
    token: str
    rowids: frozenset[int]
    old_unit_keys: frozenset[str]
    new_unit_keys: frozenset[str]
    header_tables: frozenset[str]


def route_changes(mirror: NativeMirror, capture: PreparedCapture, *,
                  media_message_ids: Iterable[int] = ()) -> WhatsAppSelection:
    """Select complete stanza groups, both media owners, and dependent replies.

    Auxiliary database and file coverage must be proved by the caller separately.
    This only classifies the exact pending ChatStorage delta, never a baseline.
    """
    if capture.baseline:
        raise RoutingUnavailable('a WhatsApp baseline requires full extraction')
    for name, table, fields in (
        ('cs_wa_route_group', 'ZWAMESSAGE', 'ZCHATSESSION,ZSTANZAID'),
        ('cs_wa_route_parent', 'ZWAMESSAGE', 'ZPARENTMESSAGE'),
        ('cs_wa_route_media', 'ZWAMEDIAITEM', 'ZMESSAGE'),
        ('cs_wa_route_data', 'ZWAMESSAGEDATAITEM', 'ZMESSAGE'),
    ):
        mirror.conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {table}({fields})')
    selected = set(media_message_ids)
    old_keys: set[str] = set()
    groups: set[tuple[int | None, str]] = set()
    chats: set[int] = set()
    headers: set[str] = set()
    changed_new_ids: set[int] = set()

    def group(row):
        chat, stanza = row.get('ZCHATSESSION'), row.get('ZSTANZAID')
        if chat is not None and type(chat) is not int:
            raise RoutingUnavailable('chat dependency is not an integer')
        if stanza is not None and not isinstance(stanza, str):
            raise RoutingUnavailable('stanza dependency is not text')
        if stanza:
            groups.add((chat, stanza))
            return f'{chat}:{stanza}'
        return None

    with mirror.index.snapshot() as reader:
        if mirror.pending(reader=reader) != capture:
            raise RoutingUnavailable('routing requires the exact pending native point')
        columns = {row['name']: json.loads(row['columns']) for row in reader.execute('SELECT name,columns FROM cs_native_mirror_tables')}
        for change in mirror.changes(capture.token, reader=reader):
            if change.table not in REQUIRED:
                raise RoutingUnavailable(f'unclassified native table change: {change.table}')
            before = dict(zip(columns[change.table], change.before)) if change.before is not None else None
            after = dict(zip(columns[change.table], change.after)) if change.after is not None else None
            if change.table == 'ZWAMESSAGE':
                selected.update(value['Z_PK'] for value in (before, after) if value is not None)
                if before is not None:
                    key = group(before)
                    if key is not None:
                        old_keys.add(key)
                if after is not None:
                    changed_new_ids.add(after['Z_PK'])
                    group(after)
            elif change.table in ('ZWAMEDIAITEM', 'ZWAMESSAGEDATAITEM'):
                for value in (before, after):
                    if value is not None and value['ZMESSAGE'] is not None:
                        selected.add(value['ZMESSAGE'])
            else:
                headers.add(change.table)
                if change.table == 'ZWACHATSESSION' and (before is None or after is None
                        or before['ZCONTACTJID'] != after['ZCONTACTJID']):
                    chats.update(value['Z_PK'] for value in (before, after) if value is not None)

        def query(sql, values):
            values = list(values)
            if any(type(value) is not int or not -(1 << 63) <= value < (1 << 63) for value in values):
                raise RoutingUnavailable('message dependency is not a signed integer row ID')
            values.sort()
            for offset in range(0, len(values), 400):
                batch = values[offset:offset + 400]
                yield from reader.execute(sql.format(','.join('?' for _ in batch)), batch)

        selected.update(row[0] for row in query('SELECT Z_PK FROM ZWAMESSAGE WHERE ZCHATSESSION IN ({})', chats))
        # A changed native row can supply the reply identity used by an unchanged child.
        selected.update(row[0] for row in query('SELECT Z_PK FROM ZWAMESSAGE WHERE ZPARENTMESSAGE IN ({})', selected))
        for row in query('SELECT ZCHATSESSION,ZSTANZAID FROM ZWAMESSAGE WHERE Z_PK IN ({})', selected):
            group(dict(row))
        for chat, stanza in groups:
            selected.update(row[0] for row in reader.execute(
                'SELECT Z_PK FROM ZWAMESSAGE WHERE ZCHATSESSION IS ? AND ZSTANZAID=?', (chat, stanza)))
        new_keys = set()
        for row in query('SELECT Z_PK,ZCHATSESSION,ZSTANZAID FROM ZWAMESSAGE WHERE Z_PK IN ({})', selected):
            key = group(dict(row))
            if key is not None:
                new_keys.add(key)
                if row['Z_PK'] not in changed_new_ids:
                    old_keys.add(key)
        return WhatsAppSelection(capture.token, frozenset(selected), frozenset(old_keys),
                                 frozenset(new_keys), frozenset(headers))


def headers_unchanged(mirror: NativeMirror, capture: PreparedCapture, *, selected_events: bool = False, selected_chats: bool = False) -> bool:
    """Prove unchanged ChatStorage header inputs; caller must also bind auxiliary evidence.

    Group events may be excluded only when the caller separately proves and
    withdraws their exact point-bound key coverage.
    Message-derived headers depend on distinct sender/recipient JIDs and incoming
    recipient JIDs. Seek only values contributed by changed rows, excluding those
    rows when reconstructing prior membership. No source tables are scanned.
    """
    if capture.baseline:
        return False
    with mirror.index.snapshot() as reader:
        if mirror.pending(reader=reader) != capture:
            raise RoutingUnavailable('header reuse requires the exact pending native point')
        columns = None
        before, after = [], []
        for change in mirror.changes(capture.token, reader=reader):
            if selected_chats and change.table == 'ZWACHATSESSION':
                continue
            if selected_events and change.table == 'ZWAGROUPMEMBERSCHANGE':
                continue
            if change.table not in ('ZWAMESSAGE', 'ZWAMEDIAITEM', 'ZWAMESSAGEDATAITEM'):
                return False
            if change.table != 'ZWAMESSAGE':
                continue
            if columns is None:
                columns = json.loads(reader.execute(
                    "SELECT columns FROM cs_native_mirror_tables WHERE name='ZWAMESSAGE'").fetchone()[0])
            if change.before is not None:
                before.append(dict(zip(columns, change.before, strict=True)))
            if change.after is not None:
                after.append(dict(zip(columns, change.after, strict=True)))
        for field, incoming in (('ZFROMJID', False), ('ZTOJID', False), ('ZTOJID', True)):
            index = 'cs_jid_' + field.lower() + ('_incoming' if incoming else '')
            if not reader.execute("SELECT 1 FROM sqlite_schema WHERE type='index' AND name=?", (index,)).fetchone():
                return False
            previous = {row[field] for row in before if row[field] is not None
                        and (not incoming or row['ZISFROMME'] == 0)}
            current = {row[field] for row in after if row[field] is not None
                       and (not incoming or row['ZISFROMME'] == 0)}
            if any(not isinstance(value, str) for value in previous | current):
                return False
            condition = ' AND m.ZISFROMME=0' if incoming else ''
            query = f'SELECT 1 FROM ZWAMESSAGE m INDEXED BY {index} WHERE m.{field}=?{condition}'
            for value in previous | current:
                old_present = value in previous
                if not old_present:
                    old_present = reader.execute(query + " AND NOT EXISTS(SELECT 1 FROM cs_native_mirror_changes d "
                        "WHERE d.name='ZWAMESSAGE' AND d.row_id=m.Z_PK) LIMIT 1", (value,)).fetchone() is not None
                new_present = value in current
                if not new_present:
                    new_present = reader.execute(query + ' LIMIT 1', (value,)).fetchone() is not None
                if old_present != new_present:
                    return False
        return True
