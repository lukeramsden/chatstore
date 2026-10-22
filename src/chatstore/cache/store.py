"""Cache store: transactional writes, revision tracking, and read queries.

Writers are serialised with BEGIN IMMEDIATE; readers use WAL snapshots.
"""

from __future__ import annotations

import contextlib
import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, NamedTuple

from ..canonical.records import now_ms
from ..identity.urn import revision_digest
from . import freshness, fts
from .migrations import migrate


def _j(o: Any) -> str:
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class SourceObservation(NamedTuple):
    """One sighting of a canonical entity in a source table."""

    entity_urn: str
    source: str
    account_scope: str
    native_table: str
    native_row_ids: list[Any]
    native_key: str | None
    fingerprint: str
    adapter_version: str
    observed_at: int
    sync_run: str | None
    minted: bool = False
    present: str = "present"

    def row(self) -> tuple[Any, ...]:
        return (self.entity_urn, self.source, self.account_scope, self.native_table, _j(self.native_row_ids), self.native_key,
                1 if self.minted else 0, self.fingerprint, self.adapter_version, self.observed_at, self.sync_run, self.present)


class Cache:
    def __init__(self, path: Path | str, *, readonly: bool = False):
        self.path = Path(path)
        uri = f"file:{self.path}?mode={'ro' if readonly else 'rwc'}"
        self.conn = sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        if not readonly:
            # Durable commits include the preservation journal and its consumer checkpoints.
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA cache_size=-65536")
            self.conn.execute("PRAGMA temp_store=MEMORY")
        self.readonly = readonly
        if not readonly:
            migrate(self.conn)
        self.has_covering_heads = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name='records_heads'").fetchone() is not None
        self.has_observation_refreshes = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='freshness_ranges'").fetchone() is not None

        self.has_message_counts = self.conn.execute(
            "SELECT 1 FROM meta WHERE key='message_counts_version' AND value='1'").fetchone() is not None

    def close(self) -> None:
        self.conn.close()

    # ---- transactions -------------------------------------------------------------------

    @contextmanager
    def write(self) -> Iterator[sqlite3.Connection]:
        if self.readonly:
            raise RuntimeError("cache opened read-only")
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    @contextmanager
    def bulk(self) -> Iterator[None]:
        """Tune for a long run of large write transactions (sync, import): defer WAL checkpoints
        while retaining the normal page-cache limit; checkpoint and truncate the WAL on exit."""
        if self.readonly:
            raise RuntimeError("cache opened read-only")
        self.conn.execute("PRAGMA wal_autocheckpoint=0")
        try:
            yield
        finally:
            self.conn.execute("PRAGMA wal_autocheckpoint=1000")
            self.conn.execute("PRAGMA cache_size=-65536")
            with contextlib.suppress(sqlite3.OperationalError):
                self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    # ---- writes -------------------------------------------------------------------------

    def upsert(
        self,
        rec: dict[str, Any],
        *,
        observed_at: int | None = None,
        sync_run: str | None = None,
        prior_digests: set[str] | None = None,
        origin: str = "sync",
    ) -> str:
        """Insert or update the current view of a record. Must run inside write().

        Returns 'inserted', 'updated', 'unchanged', 'older', or 'conflict'.

        A revision digest identifies *content*; the head of a URN is the content most recently
        observed. For origin='import', a differing current digest is only replaced when it appears in
        `prior_digests` (the importing archive's known history for this URN) and the incoming
        observation is newer; an older observation from the same history returns 'older'. A current
        digest outside the archive's history records a conflict and keeps the current record.
        """
        head = self.current_heads([rec["urn"]]).get(rec["urn"])
        return self._upsert(rec, head, observed_at, sync_run, prior_digests, origin)

    def current_heads(self, urns: Iterable[str]) -> dict[str, tuple[str, int]]:
        """(revision digest, observed_at) per URN (missing URNs are absent from the result)."""
        out: dict[str, tuple[str, int]] = {}
        batch = list(urns)
        for i in range(0, len(batch), 500):
            chunk = batch[i:i + 500]
            if self.has_observation_refreshes:
                at = freshness.time_sql('record', 'k.id')
                index = 'INDEXED BY records_heads' if self.has_covering_heads else ''
                q = f"""SELECT r.urn,r.revision_digest,max(r.observed_at,coalesce({at},r.observed_at)) FROM records r {index}
                    LEFT JOIN entity_keys k ON k.urn=r.urn WHERE r.urn IN ({','.join('?' * len(chunk))})"""
            else:
                q = f"SELECT urn,revision_digest,observed_at FROM records WHERE urn IN ({','.join('?' * len(chunk))})"
            out.update((r[0], (r[1], r[2])) for r in self.conn.execute(q, chunk))
        return out

    def upsert_many(self, recs: list[dict[str, Any]], *, observed_at: int | None = None,
                    sync_run: str | None = None, origin: str = "sync") -> dict[str, int]:
        """Batch form of upsert(): one lookup for the whole batch. Must run inside write()."""
        observed_at = observed_at if observed_at is not None else now_ms()
        heads = self.current_heads(r['urn'] for r in recs)
        outcomes: dict[str, int] = {}
        unchanged = []
        for rec in recs:
            head = heads.get(rec['urn'])
            o = self._upsert(rec, head, observed_at, sync_run, None, origin, refresh=False)
            if o == 'unchanged':
                if head is not None and observed_at >= head[1]:
                    unchanged.append(rec['urn'])
                heads[rec['urn']] = (rec['revision_digest'], max(head[1] if head else 0, observed_at))
            elif o in ('inserted', 'updated'):
                heads[rec['urn']] = (rec['revision_digest'], observed_at)
            outcomes[o] = outcomes.get(o, 0) + 1
        from .changefeed import refresh_records
        refresh_records(self.conn, unchanged, observed_at, sync_run)
        return outcomes

    def _upsert(self, rec: dict[str, Any], head: tuple[str, int] | None, observed_at: int | None, sync_run: str | None,
                prior_digests: set[str] | None, origin: str, *, refresh: bool = True) -> str:
        conn = self.conn
        observed_at = observed_at if observed_at is not None else now_ms()
        cur_digest, cur_observed = head if head else (None, 0)
        digest = rec.get("revision_digest") or revision_digest(rec)
        rec["revision_digest"] = digest
        urn = rec["urn"]
        kind = rec["entity"]
        if cur_digest == digest:
            if refresh and observed_at >= cur_observed:
                from .changefeed import refresh_records
                refresh_records(conn, [urn], observed_at, sync_run)
            return 'unchanged'
        stored = dict(rec)
        stored["observed_at"] = observed_at
        stored["sync_run"] = sync_run
        if cur_digest is None:
            conn.execute(
                "INSERT INTO records(urn, kind, source, account_scope, revision_digest, observed_at, sync_run, tombstoned, record) VALUES (?,?,?,?,?,?,?,?,?)",
                (urn, kind, rec.get("source"), rec.get("account_scope"), digest, observed_at, sync_run,
                 1 if rec.get("tombstone") else 0, _j(stored)),
            )
            self._add_revision(urn, kind, digest, observed_at, sync_run, None, stored)
            self._index(kind, stored, new=True)
            return "inserted"
        # Differing content.
        if origin == "import" and cur_digest not in (prior_digests or set()):
            self.add_conflict("content", urn, {
                "current_digest": cur_digest, "incoming_digest": digest,
                "incoming_observed_at": observed_at, "incoming_sync_run": sync_run,
            })
            # keep the incoming content as a non-current revision for inspection
            self._add_revision(urn, kind, digest, observed_at, sync_run, None, stored, current=False)
            return "conflict"
        if origin == "import" and observed_at < cur_observed:
            return "older"  # the same history already moved past this observation
        conn.execute(
            "UPDATE records SET revision_digest=?, observed_at=?, sync_run=?, tombstoned=?, record=?, source=?, account_scope=? WHERE urn=?",
            (digest, observed_at, sync_run, 1 if rec.get("tombstone") else 0, _j(stored), rec.get("source"), rec.get("account_scope"), urn),
        )
        self._add_revision(urn, kind, digest, observed_at, sync_run, cur_digest, stored)
        self._index(kind, stored)
        return "updated"

    def _add_revision(self, urn: str, kind: str, digest: str, observed_at: int, sync_run: str | None,
                      supersedes: str | None, stored: dict[str, Any], *, current: bool = True) -> None:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO revisions(entity_urn, revision_digest, entity_kind, observed_at, sync_run, supersedes, record) VALUES (?,?,?,?,?,?,?)",
            (urn, digest, kind, observed_at, sync_run, supersedes, _j(stored)),
        )
        if cur.rowcount == 1:
            if kind == "messages" and cur.lastrowid is not None:
                fts.add_revision_text(self.conn, cur.lastrowid, stored.get("text"))
        elif current:
            # Content seen before is the head again (A -> B -> A): move its revision row to the head.
            self.conn.execute(
                "UPDATE revisions SET observed_at=?, sync_run=?, supersedes=? WHERE entity_urn=? AND revision_digest=? AND observed_at<?",
                (observed_at, sync_run, supersedes, urn, digest, observed_at))

    def add_revision_row(self, rv: dict[str, Any]) -> None:
        """Import a revision row exported by another cache (`records/revisions.jsonl`). Must run inside write()."""
        self._add_revision(rv["entity_urn"], rv["entity_kind"], rv["revision_digest"], int(rv["observed_at"]), rv.get("sync_run"),
                           rv.get("supersedes"), rv["record"])

    def set_head(self, urn: str, digest: str, observed_at: int) -> dict[str, Any] | None:
        """Make a stored revision the current record (conflict resolution). Returns the record or None."""
        rev = self.revision_record(urn, digest)
        if rev is None:
            return None
        cur = self.conn.execute("SELECT revision_digest FROM records WHERE urn=?", (urn,)).fetchone()
        self.conn.execute("UPDATE records SET revision_digest=?, record=?, observed_at=? WHERE urn=?", (digest, _j(rev), observed_at, urn))
        self.conn.execute("UPDATE revisions SET observed_at=?, supersedes=? WHERE entity_urn=? AND revision_digest=?",
                          (observed_at, cur[0] if cur and cur[0] != digest else None, urn, digest))
        self._index(rev["entity"], rev)
        return rev

    def _index(self, kind: str, r: dict[str, Any], *, new: bool = False) -> None:
        c = self.conn
        urn = r["urn"]
        if kind == "messages":
            sent = r.get("sent_at") or r.get("received_at") or {}
            rowid = c.execute(
                """INSERT INTO messages_idx(urn, chat_urn, sender_urn, utc_ms, kind, transport, source, account_scope, has_attachments, is_from_me)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(urn) DO UPDATE SET chat_urn=excluded.chat_urn, sender_urn=excluded.sender_urn, utc_ms=excluded.utc_ms,
                     kind=excluded.kind, transport=excluded.transport, source=excluded.source, account_scope=excluded.account_scope,
                     has_attachments=excluded.has_attachments, is_from_me=excluded.is_from_me
                   RETURNING id""",
                (urn, r.get("chat_urn"), r.get("sender_urn"), sent.get("utc_ms"), r["kind"], r["transport"],
                 r["source"], r["account_scope"], 1 if r.get("has_attachments") else 0, 1 if r.get("is_from_me") else 0),
            ).fetchone()[0]
            fts.set_message_text(c, rowid, None if r.get("tombstone") else r.get("text"), fresh=new)
        elif kind == "events":
            at = r.get("at") or {}
            c.execute("INSERT OR REPLACE INTO events_idx(urn, target_urn, kind, at_ms, actor_urn) VALUES (?,?,?,?,?)",
                      (urn, r.get("target_urn"), r["kind"], at.get("utc_ms"), r.get("actor_urn")))
        elif kind == "attachments":
            c.execute("INSERT OR REPLACE INTO attachments_idx(urn, message_urn, availability, blob_sha256) VALUES (?,?,?,?)",
                      (urn, r.get("message_urn"), r["availability"], r.get("blob_sha256")))
        elif kind == "message_parts":
            c.execute("INSERT OR REPLACE INTO parts_idx(urn, message_urn, idx) VALUES (?,?,?)",
                      (urn, r["message_urn"], r["index"]))
        elif kind == "chat_memberships":
            c.execute("INSERT OR REPLACE INTO memberships_idx(urn, chat_urn, identity_urn) VALUES (?,?,?)",
                      (urn, r["chat_urn"], r["identity_urn"]))
        elif kind == "identity_links":
            c.execute("INSERT OR REPLACE INTO links_idx(urn, person_urn, identity_urn, state) VALUES (?,?,?,?)",
                      (urn, r["person_urn"], r["identity_urn"], r["state"]))
        elif kind == "aliases":
            c.execute("INSERT OR REPLACE INTO aliases_idx(urn, old_urn, new_urn) VALUES (?,?,?)",
                      (urn, r["old_urn"], r["new_urn"]))

    _OBSERVE_SQL = """INSERT INTO source_observations(entity_urn, source, account_scope, native_table, native_row_ids, native_key, minted, fingerprint, adapter_version, observed_at, sync_run, present)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(entity_urn, source, account_scope, native_table) DO UPDATE SET
                 native_row_ids=excluded.native_row_ids, native_key=excluded.native_key, minted=excluded.minted,
                 fingerprint=excluded.fingerprint,
                 adapter_version=excluded.adapter_version, observed_at=excluded.observed_at,
                 sync_run=excluded.sync_run, present=excluded.present"""

    def observe_many(self, rows: Iterable[SourceObservation]) -> None:
        """Record sightings from a sync run (the live source always wins). Must run inside write()."""
        rows = list(rows)
        semantic_change = ' OR '.join(f'source_observations.{k} IS NOT excluded.{k}' for k in (
            'native_row_ids', 'native_key', 'minted', 'fingerprint', 'adapter_version', 'present'))
        self.conn.executemany(self._OBSERVE_SQL + ' WHERE ' + semantic_change, [r.row() for r in rows])
        from .changefeed import refresh
        refresh(self.conn, rows)

    def observe(self, obs: SourceObservation) -> None:
        self.observe_many([obs])

    def merge_observations(self, rows: Iterable[SourceObservation]) -> None:
        """Record sightings restored from an archive: only newer observations replace existing ones."""
        at = freshness.time_sql('observation', 'source_observations.observation_id')
        self.conn.executemany(self._OBSERVE_SQL + f" WHERE excluded.observed_at >= max(source_observations.observed_at,coalesce({at},source_observations.observed_at))",
                              [r.row() for r in rows])

    def mark_absent(self, source: str, account_scope: str, native_table: str, seen_before: int,
                    sync_run: str | None, state: str = "absent_from_source") -> int:
        """Mark observations of `native_table` not touched since `seen_before` as absent."""
        at = freshness.time_sql('observation', 'source_observations.observation_id')
        cur = self.conn.execute(
            f"""UPDATE source_observations SET present=?,sync_run=?,observed_at=?
                WHERE source=? AND account_scope=? AND native_table=? AND present='present'
                AND max(observed_at,coalesce({at},observed_at))<?""",
            (state, sync_run, seen_before, source, account_scope, native_table, seen_before))
        return int(cur.rowcount)

    def add_conflict(self, kind: str, entity_urn: str | None, detail: dict[str, Any]) -> int:
        cur = self.conn.execute(
            "INSERT INTO conflicts(kind, entity_urn, detail, created_at) VALUES (?,?,?,?)",
            (kind, entity_urn, _j(detail), now_ms()),
        )
        return int(cur.lastrowid or 0)

    def set_checkpoint(self, source: str, scope: str, key: str, value: Any) -> None:
        self.conn.execute(
            "INSERT INTO checkpoints(source, account_scope, key, value) VALUES (?,?,?,?) ON CONFLICT DO UPDATE SET value=excluded.value",
            (source, scope, key, _j(value)),
        )

    def get_checkpoint(self, source: str, scope: str, key: str) -> Any:
        row = self.conn.execute("SELECT value FROM checkpoints WHERE source=? AND account_scope=? AND key=?",
                                (source, scope, key)).fetchone()
        return json.loads(row[0]) if row else None

    def clear_checkpoints(self, source: str, scope: str) -> None:
        self.conn.execute("DELETE FROM checkpoints WHERE source=? AND account_scope=?", (source, scope))

    def rebuild_fts(self) -> int:
        with self.write():
            return fts.rebuild(self.conn)

    # ---- reads --------------------------------------------------------------------------

    def get(self, urn: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT record,observed_at,sync_run FROM records WHERE urn=?", (urn,)).fetchone()
        return self.record_freshness(json.loads(row[0]) | {"observed_at": row[1], "sync_run": row[2]}) if row else None

    def get_many(self, urns: Iterable[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        urns = [u for u in set(urns) if u]
        for i in range(0, len(urns), 500):
            chunk = urns[i:i + 500]
            q = f"SELECT urn,record,observed_at,sync_run FROM records WHERE urn IN ({','.join('?' * len(chunk))})"
            for r in self.conn.execute(q, chunk):
                out[r[0]] = self.record_freshness(json.loads(r[1]) | {"observed_at": r[2], "sync_run": r[3]})
        return out

    def revisions_of(self, urn: str) -> list[dict[str, Any]]:
        return [
            {"revision_digest": r[0], "observed_at": r[1], "sync_run": r[2], "supersedes": r[3]}
            for r in self.conn.execute(
                "SELECT revision_digest, observed_at, sync_run, supersedes FROM revisions WHERE entity_urn=? ORDER BY observed_at, revision_digest",
                (urn,))
        ]

    def revision_record(self, urn: str, digest: str) -> dict[str, Any] | None:
        row = self.conn.execute("SELECT record FROM revisions WHERE entity_urn=? AND revision_digest=?", (urn, digest)).fetchone()
        return json.loads(row[0]) if row else None

    def aliases_from(self, old_urn: str) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT new_urn FROM aliases_idx WHERE old_urn=?", (old_urn,))]

    def aliases_to(self, new_urn: str) -> list[str]:
        return [r[0] for r in self.conn.execute("SELECT old_urn FROM aliases_idx WHERE new_urn=?", (new_urn,))]

    def observation_freshness(self, observation_id: int, observation: dict[str, Any]) -> dict[str, Any]:
        if not self.has_observation_refreshes:
            return observation
        return freshness.lookup(self.conn, 'observation', observation_id, observation)

    def record_freshness(self, rec: dict[str, Any]) -> dict[str, Any]:
        if not self.has_observation_refreshes:
            return rec
        key = self.conn.execute('SELECT id FROM entity_keys WHERE urn=?', (rec['urn'],)).fetchone()
        return freshness.lookup(self.conn, 'record', key[0], rec) if key else rec

    def observations_of(self, urn: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT rowid AS observation_id, source, account_scope, native_table, native_row_ids, native_key, minted, fingerprint, adapter_version, observed_at, sync_run, present FROM source_observations WHERE entity_urn=?",
            (urn,))
        return [self.observation_freshness(r["observation_id"], dict(r) | {"native_row_ids": json.loads(r["native_row_ids"])}) for r in rows]

    def iter_records(self, kind: str, *, source: str | None = None, scope: str | None = None,
                     include_tombstoned: bool = True) -> Iterator[dict[str, Any]]:
        q = "SELECT record,observed_at,sync_run FROM records WHERE kind=?"
        args: list[Any] = [kind]
        if source:
            q += " AND source=?"
            args.append(source)
        if scope:
            q += " AND account_scope=?"
            args.append(scope)
        if not include_tombstoned:
            q += " AND tombstoned=0"
        q += " ORDER BY urn"
        for r in self.conn.execute(q, args):
            yield self.record_freshness(json.loads(r[0]) | {"observed_at": r[1], "sync_run": r[2]})

    def counts(self) -> dict[str, int]:
        return {r[0]: r[1] for r in self.conn.execute("SELECT kind, count(*) FROM records GROUP BY kind ORDER BY kind")}

    def coverage(self, source: str | None = None, scope: str | None = None) -> dict[str, Any]:
        from .coverage import read
        if self.has_message_counts:
            return read(self.conn, source, scope)
        # Read-only access to an older cache cannot install its count projection.
        q = "SELECT min(utc_ms),max(utc_ms),count(*) FROM messages_idx WHERE kind='message'"
        args = []
        for column, value in (('source', source), ('account_scope', scope)):
            if value is not None:
                q += f' AND {column}=?'
                args.append(value)
        row = self.conn.execute(q, args).fetchone()
        return {'earliest_utc_ms': row[0], 'latest_utc_ms': row[1], 'messages': row[2]}

    def latest_sync_runs(self, scopes: Iterable[tuple[str, str]] | None = None) -> list[dict[str, Any]]:
        if scopes is None:
            scopes = [tuple(r) for r in self.conn.execute(
                "SELECT DISTINCT source,account_scope FROM records WHERE kind='sync_runs'")]
        out = []
        for source, scope in sorted(scopes):
            row = self.conn.execute("""SELECT record FROM records WHERE kind='sync_runs'
                AND source=? AND account_scope=? ORDER BY observed_at DESC,urn DESC LIMIT 1""",
                (source, scope)).fetchone()
            if row:
                out.append(json.loads(row[0]))
        return out

    def conflicts(self, *, unresolved_only: bool = True) -> list[dict[str, Any]]:
        q = "SELECT id, kind, entity_urn, detail, created_at, resolved_at, resolution FROM conflicts"
        if unresolved_only:
            q += " WHERE resolved_at IS NULL"
        return [dict(r) | {"detail": json.loads(r["detail"])} for r in self.conn.execute(q + " ORDER BY id")]

    def open_conflict_count(self) -> int:
        return int(self.conn.execute("SELECT count(*) FROM conflicts WHERE resolved_at IS NULL").fetchone()[0])

    def identities_of_person(self, person_urn: str) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT identity_urn FROM links_idx WHERE person_urn=? AND state IN ('confirmed','suggested')", (person_urn,))]
