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
        for row in query('SELECT ZCHATSESSION,ZSTANZAID FROM ZWAMESSAGE WHERE Z_PK IN ({})', selected):
            key = group(dict(row))
            if key is not None:
                new_keys.add(key)
        old_keys.update(new_keys)
        return WhatsAppSelection(capture.token, frozenset(selected), frozenset(old_keys),
                                 frozenset(new_keys), frozenset(headers))
