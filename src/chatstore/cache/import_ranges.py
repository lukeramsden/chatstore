"""Incremental producer-key translation for exact imported sightings."""

from __future__ import annotations

import sqlite3


def install(conn: sqlite3.Connection) -> None:
    ddl = '''
        CREATE TABLE IF NOT EXISTS import_range_ready (
            producer TEXT NOT NULL, kind TEXT NOT NULL, PRIMARY KEY(producer,kind));
        CREATE TABLE IF NOT EXISTS import_range_dirty (
            producer TEXT NOT NULL, kind TEXT NOT NULL, id INTEGER NOT NULL,
            PRIMARY KEY(producer,kind,id));
        CREATE TABLE IF NOT EXISTS import_key_ranges (
            producer TEXT NOT NULL, kind TEXT NOT NULL, first_id INTEGER NOT NULL,
            last_id INTEGER NOT NULL, offset INTEGER NOT NULL, eligible INTEGER NOT NULL,
            PRIMARY KEY(producer,kind,first_id));
        CREATE INDEX IF NOT EXISTS entity_ids_local ON entity_ids(local_id);
        CREATE INDEX IF NOT EXISTS observation_ids_local ON observation_ids(local_id);
    '''
    for statement in ddl.split(';'):
        if statement.strip():
            conn.execute(statement)
    for table, producer, key, kind in (
        ('entity_ids', 'producer', 'entity_id', 'record'),
        ('observation_ids', 'feed_id', 'observation_id', 'observation'),
    ):
        for action in ('INSERT', 'UPDATE', 'DELETE'):
            refs = ('OLD', 'NEW') if action == 'UPDATE' else ('OLD',) if action == 'DELETE' else ('NEW',)
            body = ''.join(f"INSERT INTO import_range_dirty SELECT {r}.{producer},'{kind}',{r}.{key} WHERE NOT EXISTS (SELECT 1 FROM import_range_dirty WHERE producer={r}.{producer} AND kind='{kind}' AND id={r}.{key});" for r in refs)
            conn.execute(f'CREATE TRIGGER IF NOT EXISTS range_{table}_{action} AFTER {action} ON {table} BEGIN {body} END')
    for table, kind, mappings, producer, key, lookup, condition in (
        ('records', 'record', 'entity_ids', 'producer', 'entity_id',
         '(SELECT id FROM entity_keys WHERE urn={ref}.urn)',
         'OLD.revision_digest IS NOT NEW.revision_digest'),
        ('source_observations', 'observation', 'observation_ids', 'feed_id', 'observation_id',
         '{ref}.observation_id', ' OR '.join(f'OLD.{k} IS NOT NEW.{k}' for k in (
             'entity_urn', 'source', 'account_scope', 'native_table', 'native_row_ids', 'native_key',
             'minted', 'fingerprint', 'adapter_version', 'present'))),
    ):
        for action in ('INSERT', 'UPDATE', 'DELETE'):
            refs = ('OLD', 'NEW') if action == 'UPDATE' else ('OLD',) if action == 'DELETE' else ('NEW',)
            body = ''.join(f"INSERT INTO import_range_dirty SELECT {producer},'{kind}',{key} FROM {mappings} m WHERE local_id={lookup.format(ref=r)} AND NOT EXISTS (SELECT 1 FROM import_range_dirty d WHERE d.producer=m.{producer} AND d.kind='{kind}' AND d.id=m.{key});" for r in refs)
            when = f'WHEN {condition}' if action == 'UPDATE' else ''
            conn.execute(f'CREATE TRIGGER IF NOT EXISTS range_{table}_{action} AFTER {action} ON {table} {when} BEGIN {body} END')


def _mapping_sql(kind: str) -> str:
    if kind == 'record':
        return '''SELECT m.entity_id,m.local_id,coalesce(r.revision_digest=m.head_digest,0)
            FROM entity_ids m LEFT JOIN entity_keys k ON k.id=m.local_id
            LEFT JOIN records r ON r.urn=k.urn WHERE m.producer=?'''
    return '''SELECT m.observation_id,m.local_id,coalesce(
        o.entity_urn=json_extract(m.evidence,'$.entity_urn')
        AND o.source=json_extract(m.evidence,'$.source')
        AND o.account_scope=json_extract(m.evidence,'$.account_scope')
        AND o.native_table=json_extract(m.evidence,'$.native_table')
        AND json(o.native_row_ids)=json_extract(m.evidence,'$.native_row_ids')
        AND o.native_key IS json_extract(m.evidence,'$.native_key')
        AND o.minted=json_extract(m.evidence,'$.minted')
        AND o.fingerprint=json_extract(m.evidence,'$.fingerprint')
        AND o.adapter_version=json_extract(m.evidence,'$.adapter_version')
        AND o.present=json_extract(m.evidence,'$.present'),0)
        FROM observation_ids m LEFT JOIN source_observations o ON o.observation_id=m.local_id
        WHERE m.feed_id=?'''


def _replace(conn: sqlite3.Connection, producer: str, kind: str, key: int,
             value: tuple[int, int] | None) -> None:
    prefix = (producer, kind)
    old = conn.execute('''SELECT first_id,last_id,offset,eligible FROM import_key_ranges
        WHERE producer=? AND kind=? AND first_id<=? ORDER BY first_id DESC LIMIT 1''', (*prefix, key)).fetchone()
    if old and old[1] >= key:
        lo, hi, offset, eligible = old
        conn.execute('DELETE FROM import_key_ranges WHERE producer=? AND kind=? AND first_id=?', (*prefix, lo))
        for a, b in ((lo, key - 1), (key + 1, hi)):
            if a <= b:
                conn.execute('INSERT INTO import_key_ranges VALUES (?,?,?,?,?,?)', (*prefix, a, b, offset, eligible))
    if value is None:
        return
    offset, eligible = value
    lo = hi = key
    left = conn.execute('''SELECT first_id,last_id,offset,eligible FROM import_key_ranges
        WHERE producer=? AND kind=? AND first_id<? ORDER BY first_id DESC LIMIT 1''', (*prefix, key)).fetchone()
    right = conn.execute('''SELECT first_id,last_id,offset,eligible FROM import_key_ranges
        WHERE producer=? AND kind=? AND first_id=?''', (*prefix, key + 1)).fetchone()
    for neighbor in (left, right):
        if neighbor and (neighbor[1] == key - 1 or neighbor[0] == key + 1) and tuple(neighbor[2:]) == value:
            lo, hi = min(lo, neighbor[0]), max(hi, neighbor[1])
            conn.execute('DELETE FROM import_key_ranges WHERE producer=? AND kind=? AND first_id=?', (*prefix, neighbor[0]))
    conn.execute('INSERT INTO import_key_ranges VALUES (?,?,?,?,?,?)', (*prefix, lo, hi, offset, eligible))


def translate(conn: sqlite3.Connection, producer: str, kind: str, ranges: list[list[int]]) -> list[list[int]]:
    """Refresh changed mapping points, then translate using affine intervals only.

    Caller holds the import writer transaction. Unknown keys fail the entire import.
    """
    prefix = (producer, kind)
    key_column = 'm.entity_id' if kind == 'record' else 'm.observation_id'
    sql = _mapping_sql(kind)
    if not conn.execute('SELECT 1 FROM import_range_ready WHERE producer=? AND kind=?', prefix).fetchone():
        # One-time projection for databases imported before this index existed.
        pending = None
        for key, local, eligible in conn.execute(sql + f' ORDER BY {key_column}', (producer,)):
            value = (local - key, eligible)
            if pending and pending[1] + 1 == key and pending[2:] == value:
                pending = (pending[0], key, *value)
            else:
                if pending:
                    conn.execute('INSERT INTO import_key_ranges VALUES (?,?,?,?,?,?)', (*prefix, *pending))
                pending = (key, key, *value)
        if pending:
            conn.execute('INSERT INTO import_key_ranges VALUES (?,?,?,?,?,?)', (*prefix, *pending))
        conn.execute('INSERT INTO import_range_ready VALUES (?,?)', prefix)
    else:
        dirty = list(conn.execute('SELECT id FROM import_range_dirty WHERE producer=? AND kind=? ORDER BY id', prefix))
        for (key,) in dirty:
            row = conn.execute(sql + f' AND {key_column}=?', (producer, key)).fetchone()
            _replace(conn, producer, kind, key, (row[1] - key, row[2]) if row else None)
    conn.execute('DELETE FROM import_range_dirty WHERE producer=? AND kind=?', prefix)
    translated = []
    for lo, hi in ranges:
        covered = lo
        # The predecessor may start before lo; all later overlaps start inside the input.
        first = conn.execute('''SELECT first_id FROM import_key_ranges WHERE producer=? AND kind=?
            AND first_id<=? ORDER BY first_id DESC LIMIT 1''', (*prefix, lo)).fetchone()
        start = first[0] if first else lo
        for a, b, offset, eligible in conn.execute('''SELECT first_id,last_id,offset,eligible
            FROM import_key_ranges WHERE producer=? AND kind=? AND first_id BETWEEN ? AND ?
            ORDER BY first_id''', (*prefix, start, hi)):
            a, b = max(a, lo), min(b, hi)
            if b < a:
                continue
            if a != covered:
                raise ValueError('sighting range references unknown producer keys')
            covered = b + 1
            if eligible:
                translated.append([a + offset, b + offset])
        if covered != hi + 1:
            raise ValueError('sighting range references unknown producer keys')
    # Local IDs need not preserve producer order; merge overlapping translated intervals.
    merged: list[list[int]] = []
    for a, b in sorted(translated):
        if merged and a <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    return merged
