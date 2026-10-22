"""Transactional preservation journal. Sequence numbers never depend on message dates."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

DDL = [
    """CREATE TABLE IF NOT EXISTS freshness_ranges (
        kind TEXT NOT NULL, first_id INTEGER NOT NULL, last_id INTEGER NOT NULL,
        observed_at INTEGER NOT NULL, sync_run TEXT, PRIMARY KEY(kind, first_id)
    )""",
    "CREATE INDEX IF NOT EXISTS freshness_end ON freshness_ranges(kind,last_id)",
    "CREATE INDEX IF NOT EXISTS freshness_run ON freshness_ranges(sync_run,kind,first_id)",
    """CREATE TABLE IF NOT EXISTS entity_keys (
        id INTEGER PRIMARY KEY AUTOINCREMENT, urn TEXT NOT NULL UNIQUE
    )""",
    """CREATE TABLE IF NOT EXISTS entity_ids (
        producer TEXT NOT NULL, entity_id INTEGER NOT NULL, local_id INTEGER NOT NULL, head_digest TEXT,
        PRIMARY KEY(producer, entity_id)
    )""",
    """CREATE TABLE IF NOT EXISTS applied_changes (
        producer TEXT NOT NULL, sequence INTEGER NOT NULL, event_sha256 TEXT NOT NULL,
        PRIMARY KEY(producer, sequence)
    )""",
    """CREATE TABLE IF NOT EXISTS import_paths (
        path TEXT PRIMARY KEY, export_id TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS observation_refreshes (
        first_id INTEGER NOT NULL, last_id INTEGER NOT NULL, observed_at INTEGER NOT NULL, sync_run TEXT,
        PRIMARY KEY(first_id, last_id)
    )""",
    """CREATE TABLE IF NOT EXISTS verified_imports (
        export_id TEXT PRIMARY KEY, ciphertext_sha256 TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS verified_exports (
        export_id TEXT PRIMARY KEY, receipt TEXT NOT NULL
    )""",
    """CREATE INDEX IF NOT EXISTS verified_exports_stream ON verified_exports(
        json_extract(receipt, '$.feed.stream'), json_extract(receipt, '$.feed.through'))""",
    """CREATE TABLE IF NOT EXISTS delivery_checkpoints (
        destination TEXT NOT NULL, stream_id TEXT NOT NULL, sequence INTEGER NOT NULL,
        export_id TEXT NOT NULL, ciphertext_sha256 TEXT NOT NULL,
        PRIMARY KEY(destination, stream_id)
    )""",
    """CREATE TABLE IF NOT EXISTS delivery_proofs (
        destination TEXT NOT NULL, export_id TEXT NOT NULL, proof TEXT NOT NULL,
        PRIMARY KEY(destination, export_id)
    )""",
    """CREATE TABLE IF NOT EXISTS changes (
        seq INTEGER PRIMARY KEY AUTOINCREMENT, operation TEXT NOT NULL, data TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS export_consumers (
        consumer TEXT PRIMARY KEY, feed_id TEXT NOT NULL, sequence INTEGER NOT NULL,
        export_id TEXT, ciphertext_sha256 TEXT, policy TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS feed_imports (
        stream_id TEXT PRIMARY KEY, feed_id TEXT NOT NULL, sequence INTEGER NOT NULL,
        export_id TEXT NOT NULL, ciphertext_sha256 TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS observation_ids (
        feed_id TEXT NOT NULL, observation_id INTEGER NOT NULL, local_id INTEGER NOT NULL, evidence TEXT,
        PRIMARY KEY(feed_id, observation_id)
    )""",
]

RECORD = "json_object('entity_id', (SELECT id FROM entity_keys WHERE urn=NEW.urn), 'record', json(NEW.record), 'observed_at', NEW.observed_at, 'sync_run', NEW.sync_run)"
REVISION = """json_object('entity_urn', NEW.entity_urn, 'revision_digest', NEW.revision_digest,
    'entity_kind', NEW.entity_kind, 'observed_at', NEW.observed_at, 'sync_run', NEW.sync_run,
    'supersedes', NEW.supersedes, 'record', json(NEW.record))"""
OBSERVATION = """json_object('observation_id', NEW.rowid, 'entity_urn', NEW.entity_urn,
    'source', NEW.source, 'account_scope', NEW.account_scope, 'native_table', NEW.native_table,
    'native_row_ids', json(NEW.native_row_ids), 'native_key', NEW.native_key,
    'minted', json(CASE WHEN NEW.minted THEN 'true' ELSE 'false' END),
    'fingerprint', NEW.fingerprint, 'adapter_version', NEW.adapter_version,
    'observed_at', NEW.observed_at, 'sync_run', NEW.sync_run, 'present', NEW.present)"""


def install(conn: sqlite3.Connection) -> None:
    for sql in DDL:
        conn.execute(sql)
    if 'evidence' not in {r[1] for r in conn.execute('PRAGMA table_info(observation_ids)')}:
        conn.execute('ALTER TABLE observation_ids ADD COLUMN evidence TEXT')
    if conn.execute("SELECT 1 FROM meta WHERE key='feed_id'").fetchone():
        _upgrade_sightings(conn)
        return
    # One migration transaction seeds existing history before enabling capture. The seed is also
    # the baseline for a new consumer; no later export needs to rediscover retained records.
    conn.execute("BEGIN IMMEDIATE")
    try:
        if conn.execute("SELECT 1 FROM meta WHERE key='feed_id'").fetchone():
            conn.rollback()
            return
        conn.execute('INSERT OR IGNORE INTO entity_keys(urn) SELECT urn FROM records ORDER BY rowid')
        for table, operation, expression in (
            ('revisions', 'revision', REVISION), ('records', 'record', RECORD),
            ('source_observations', 'observation', OBSERVATION),
        ):
            conn.execute(f"INSERT INTO changes(operation, data) SELECT ?, {expression} FROM {table} AS NEW", (operation,))
            key_sql = "INSERT OR IGNORE INTO entity_keys(urn) VALUES (NEW.urn);" if table == 'records' else ''
            conn.execute(f"""CREATE TRIGGER journal_{table}_insert AFTER INSERT ON {table} BEGIN
                {key_sql}
                INSERT INTO changes(operation, data) VALUES ('{operation}', {expression}); END""")
            if table == 'records':
                condition = "OLD.revision_digest IS NOT NEW.revision_digest OR OLD.record IS NOT NEW.record"
                expression = f"json_set({expression}, '$.previous_digest', OLD.revision_digest)"
            elif table == 'source_observations':
                condition = " OR ".join(f"OLD.{k} IS NOT NEW.{k}" for k in (
                    'native_row_ids', 'native_key', 'minted', 'fingerprint', 'adapter_version', 'present'))
            else:
                condition = "OLD.observed_at IS NOT NEW.observed_at OR OLD.supersedes IS NOT NEW.supersedes OR OLD.record IS NOT NEW.record"
            conn.execute(f"""CREATE TRIGGER journal_{table}_update AFTER UPDATE ON {table}
                WHEN {condition} BEGIN
                INSERT INTO changes(operation, data) VALUES ('{operation}', {expression}); END""")
        conn.execute("INSERT INTO meta(key,value) VALUES ('feed_id',?)", (uuid.uuid4().urn,))
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('sightings_version','2')")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def append(conn: sqlite3.Connection, operation: str, data: dict[str, Any]) -> None:
    conn.execute("INSERT INTO changes(operation,data) VALUES (?,?)",
                 (operation, json.dumps(data, separators=(',', ':'), sort_keys=True)))


def refresh(conn: sqlite3.Connection, observations: list[Any]) -> None:
    """Encode exact sightings as contiguous local observation-ID spans, once per write batch.

    Unchanged content needs no record/revision event. A full resighting of a million consecutive
    observations is represented by one interval, while partial scans retain their exact coverage.
    """
    groups: dict[tuple[str, str, str, int, str | None], list[str]] = {}
    for obs in observations:
        key = (obs.source, obs.account_scope, obs.native_table, obs.observed_at, obs.sync_run)
        groups.setdefault(key, []).append(obs.entity_urn)
    for (source, scope, table, at, run), urns in groups.items():
        ids: list[int] = []
        for offset in range(0, len(urns), 500):
            chunk = urns[offset:offset + 500]
            placeholders = ','.join('?' * len(chunk))
            ids.extend(row[0] for row in conn.execute(f"""SELECT observation_id FROM source_observations
                WHERE entity_urn IN ({placeholders}) AND source=? AND account_scope=? AND native_table=?""",
                [*chunk, source, scope, table]))
        spans = id_ranges(ids)
        from .freshness import stamp
        stamp(conn, 'observation', spans, at, run)
        append(conn, 'observation_refresh', {'ranges': spans, 'observed_at': at, 'sync_run': run})


def id_ranges(ids: list[int]) -> list[list[int]]:
    ranges: list[list[int]] = []
    for n in sorted(set(ids)):
        if ranges and n == ranges[-1][1] + 1:
            ranges[-1][1] = n
        else:
            ranges.append([n, n])
    return ranges


def capture_identity(conn: sqlite3.Connection, identity: Any) -> None:
    data = {'scopes': identity.scopes, 'scope_mappings': identity.scope_mappings}
    serialized = json.dumps(data, separators=(',', ':'), sort_keys=True)
    previous = conn.execute("SELECT value FROM meta WHERE key='feed_identity'").fetchone()
    if previous is None or previous[0] != serialized:
        append(conn, 'identity_metadata', data)
        conn.execute("INSERT INTO meta(key,value) VALUES ('feed_identity',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (serialized,))


def refresh_records(conn: sqlite3.Connection, urns: list[str], at: int, run: str | None) -> None:
    from .freshness import stamp
    ids: list[int] = []
    for offset in range(0, len(urns), 500):
        chunk = urns[offset:offset + 500]
        ids.extend(row[0] for row in conn.execute(
            f"SELECT id FROM entity_keys WHERE urn IN ({','.join('?' * len(chunk))})", chunk))
    if ids:
        spans = id_ranges(ids)
        stamp(conn, 'record', spans, at, run)
        append(conn, 'record_refresh', {'ranges': spans, 'observed_at': at, 'sync_run': run})


def _upgrade_sightings(conn: sqlite3.Connection) -> None:
    if conn.execute("SELECT 1 FROM meta WHERE key='sightings_version' AND value='2'").fetchone():
        return
    from .freshness import stamp
    conn.execute('BEGIN IMMEDIATE')
    try:
        if conn.execute("SELECT 1 FROM meta WHERE key='sightings_version' AND value='2'").fetchone():
            conn.rollback()
            return
        conn.execute('INSERT OR IGNORE INTO entity_keys(urn) SELECT urn FROM records ORDER BY rowid')
        conn.execute("INSERT INTO changes(operation,data) SELECT 'entity_key', json_object('entity_id',k.id,'urn',k.urn,'revision_digest',r.revision_digest,'observed_at',r.observed_at,'sync_run',r.sync_run) FROM entity_keys k JOIN records r ON r.urn=k.urn ORDER BY k.id")
        for action in ('insert', 'update'):
            conn.execute(f'DROP TRIGGER journal_records_{action}')
        conn.execute(f"""CREATE TRIGGER journal_records_insert AFTER INSERT ON records BEGIN
            INSERT OR IGNORE INTO entity_keys(urn) VALUES (NEW.urn);
            INSERT INTO changes(operation,data) VALUES ('record',{RECORD}); END""")
        conn.execute(f"""CREATE TRIGGER journal_records_update AFTER UPDATE ON records
            WHEN OLD.revision_digest IS NOT NEW.revision_digest OR OLD.record IS NOT NEW.record BEGIN
            INSERT INTO changes(operation,data) VALUES ('record',json_set({RECORD}, '$.previous_digest', OLD.revision_digest)); END""")
        for row in conn.execute('SELECT first_id,last_id,observed_at,sync_run FROM observation_refreshes ORDER BY observed_at'):
            stamp(conn, 'observation', [[row[0],row[1]]], row[2], row[3])
        conn.execute("INSERT OR REPLACE INTO meta VALUES ('sightings_version','2')")
        conn.commit()
    except BaseException:
        conn.rollback()
        raise

