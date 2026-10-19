"""Indexed old/new dependency routing for a prepared Messages native mirror."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass

from ..native_mirror import NativeMirror, PreparedCapture

# Match ck_chat_id.split(';', 2)[-1], including zero/one/many delimiters.
CK_IDENTIFIER = """CASE WHEN instr(ck_chat_id,';')=0 THEN ck_chat_id
    WHEN instr(substr(ck_chat_id,instr(ck_chat_id,';')+1),';')=0
    THEN substr(ck_chat_id,instr(ck_chat_id,';')+1)
    ELSE substr(ck_chat_id,instr(ck_chat_id,';')+
        instr(substr(ck_chat_id,instr(ck_chat_id,';')+1),';')+1) END"""


class RoutingUnavailable(Exception):
    pass


@dataclass(frozen=True)
class MessagesSelection:
    token: str
    rowids: frozenset[int]
    old_unit_keys: frozenset[str]
    new_unit_keys: frozenset[str]
    inferred_chat_inputs: frozenset[str]
    header_tables: frozenset[str]


def install_indexes(conn: sqlite3.Connection) -> None:
    for name, table, columns in (
        ('cs_message_reply', 'message', 'thread_originator_guid'),
        ('cs_join_message', 'chat_message_join', 'message_id'),
        ('cs_attachment_message', 'message_attachment_join', 'message_id'),
        ('cs_attachment_owner_message', 'message_attachment_join', 'attachment_id,message_id'),
        ('cs_recoverable_message', 'chat_recoverable_message_join', 'message_id'),
        ('cs_route_message_guid', 'message', 'guid'),
        ('cs_route_media_file', 'attachment', 'filename'),
        ('cs_route_message_handle', 'message', 'handle_id'),
        ('cs_route_message_subject', 'message', 'other_handle'),
        ('cs_route_chat_members', 'chat_message_join', 'chat_id,message_id'),
        ('cs_route_ck_exact', 'message', 'ck_chat_id'),
        ('cs_route_ck_identifier', 'message', CK_IDENTIFIER),
    ):
        if conn.execute("SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?", (table,)).fetchone():
            if name == 'cs_route_message_guid':
                plan = conn.execute('EXPLAIN QUERY PLAN SELECT ROWID FROM message WHERE guid=?', ('',)).fetchall()
                if any('SEARCH ' in row[3] and 'INDEX ' in row[3] for row in plan):
                    continue  # Native UNIQUE(guid) normally supplies this lookup already.
            conn.execute(f'CREATE INDEX IF NOT EXISTS {name} ON {table}({columns})')


def route_changes(mirror: NativeMirror, capture: PreparedCapture, *,
                  media_attachment_ids: Iterable[int] = ()) -> MessagesSelection:
    """Return affected rows/old keys; canonical freshness/absence is a later step.

    Indexes are built by the baseline adapter. Missing indexes can be built once
    here for migration; schema/policy discontinuities still require reconciliation.
    """
    if capture.baseline:
        raise RoutingUnavailable('a Messages baseline requires full extraction')
    install_indexes(mirror.conn)
    selected: set[int] = set()
    handles: set[int] = set()
    chats: set[int] = set()
    attachments: set[int] = set()
    identifiers: set[str] = set()
    guids: set[str] = set()
    parents: set[str] = set()
    stub_refs: set[str] = set()
    join_messages: set[int] = set()
    presence_changes: set[int] = set()
    old_guids: dict[int, str | None] = {}
    old_ck: dict[int, str | None] = {}
    header_tables: set[str] = set()

    def row_id(value):
        if type(value) is not int or not -(1 << 63) <= value < (1 << 63):
            raise RoutingUnavailable('native dependency is not a signed integer row ID')
        return value

    def string(value):
        if value is not None and not isinstance(value, str):
            raise RoutingUnavailable('native dependency is not a text key')
        return value

    attachments.update(row_id(value) for value in media_attachment_ids)
    with mirror.index.snapshot() as reader:
        if mirror.pending(reader=reader) != capture:
            raise RoutingUnavailable('routing requires the exact pending native point')
        columns = {row['name']: json.loads(row['columns']) for row in reader.execute('SELECT name,columns FROM cs_native_mirror_tables')}
        supported = {'message', 'handle', 'chat', 'attachment', 'chat_message_join',
                     'chat_handle_join', 'message_attachment_join', 'chat_recoverable_message_join'}
        for change in mirror.changes(capture.token, reader=reader):
            table = change.table
            if table not in supported:
                raise RoutingUnavailable(f'unclassified native table change: {table}')
            before = dict(zip(columns[table], change.before)) if change.before is not None else None
            after = dict(zip(columns[table], change.after)) if change.after is not None else None
            values = [value for value in (before, after) if value is not None]
            if table == 'message':
                selected.add(change.row_id)
                old_guids[change.row_id] = string(before.get('guid')) if before else None
                old_ck[change.row_id] = string(before.get('ck_chat_id')) if before else None
                for value in values:
                    guid, parent = string(value.get('guid')), string(value.get('thread_originator_guid'))
                    if guid is not None:
                        guids.add(guid)
                    if parent is not None:
                        parents.add(parent)
                prior_ck = string(before.get('ck_chat_id')) if before else None
                next_ck = string(after.get('ck_chat_id')) if after else None
                if prior_ck != next_ck or before is None or after is None:
                    stub_refs.update(key for key in (prior_ck, next_ck) if key is not None)
                if before is None or after is None:
                    presence_changes.add(change.row_id)
            elif table == 'handle':
                if before is None or after is None or any(before.get(field) != after.get(field) for field in ('service', 'id')):
                    handles.add(change.row_id)
                header_tables.add(table)
            elif table == 'chat':
                if before is None or after is None or before.get('guid') != after.get('guid'):
                    chats.add(change.row_id)
                if before is None or after is None or any(before.get(field) != after.get(field) for field in ('guid', 'chat_identifier', 'service_name')):
                    identifiers.update(string(value.get('chat_identifier')) or '' for value in values)
                header_tables.add(table)
            elif table == 'attachment':
                attachments.add(change.row_id)
            elif table in ('chat_message_join', 'chat_recoverable_message_join'):
                ids = {row_id(value['message_id']) for value in values}
                selected.update(ids)
                if table == 'chat_message_join':
                    join_messages.update(ids)
            elif table == 'message_attachment_join':
                selected.update(row_id(value['message_id']) for value in values)
                attachments.update(row_id(value['attachment_id']) for value in values)
            else:
                header_tables.add(table)

        def query(sql, values):
            values = sorted(values)
            for offset in range(0, len(values), 400):
                batch = values[offset:offset + 400]
                yield from reader.execute(sql.format(','.join('?' for _ in batch)), batch)

        selected.update(row[0] for row in query('SELECT ROWID FROM message WHERE guid IN ({})', guids | parents))
        for field in ('handle_id', 'other_handle'):
            selected.update(row[0] for row in query(f'SELECT ROWID FROM message WHERE {field} IN ({{}})', handles))
        selected.update(row[0] for row in query('SELECT message_id FROM chat_message_join WHERE chat_id IN ({})', chats))
        selected.update(row[0] for row in query('SELECT ROWID FROM message m WHERE (' + CK_IDENTIFIER +
            ') IN ({}) AND NOT EXISTS(SELECT 1 FROM chat_message_join j WHERE j.message_id=m.ROWID)', identifiers))
        attachments.update(row[0] for row in query('SELECT attachment_id FROM message_attachment_join WHERE message_id IN ({})', presence_changes))
        selected.update(row[0] for row in query('SELECT message_id FROM message_attachment_join WHERE attachment_id IN ({})', attachments))
        for row in query('SELECT ROWID,ck_chat_id FROM message WHERE ROWID IN ({})', join_messages):
            if row[1] is not None:
                stub_refs.add(string(row[1]))
        for row in join_messages:
            previous_ck = old_ck.get(row)
            if previous_ck is not None:
                stub_refs.add(previous_ck)
        selected.update(row[0] for row in query('SELECT ROWID FROM message m WHERE ck_chat_id IN ({}) '
            'AND NOT EXISTS(SELECT 1 FROM chat_message_join j WHERE j.message_id=m.ROWID)', stub_refs))
        witness_keys = {string(row[0]) for row in query('SELECT guid FROM message WHERE ROWID IN ({})', selected)}
        witness_keys.discard(None)
        selected.update(row[0] for row in query('SELECT ROWID FROM message WHERE guid IN ({})', witness_keys))
        old_keys: set[str] = set()
        new_keys: set[str] = set()
        inference: set[str] = set()
        for row in query('SELECT ROWID,guid,ck_chat_id FROM message WHERE ROWID IN ({})', selected):
            guid, ck = string(row[1]), string(row[2])
            if guid is not None:
                new_keys.add(guid)
            previous = old_guids.get(row[0], guid)
            if previous is not None:
                old_keys.add(previous)
            inference.update(key for key in (ck, old_ck.get(row[0])) if key is not None)
        old_keys.update(value for value in old_guids.values() if value is not None)
        inference.update(value for value in old_ck.values() if value is not None)
        if inference or identifiers:
            header_tables.add('message.ck_chat_id')
        return MessagesSelection(capture.token, frozenset(row_id(value) for value in selected),
            frozenset(old_keys), frozenset(new_keys), frozenset(inference), frozenset(header_tables))
