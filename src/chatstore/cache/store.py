"""Cache store: transactional writes, revision tracking, and read queries.

Writers are serialised with BEGIN IMMEDIATE; readers use WAL snapshots.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..canonical.records import now_ms
from ..identity.urn import revision_digest
from . import fts
from .migrations import migrate


def _j(o: Any) -> str:
    return json.dumps(o, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


class Cache:
    def __init__(self, path: Path | str, *, readonly: bool = False):
        self.path = Path(path)
        uri = f"file:{self.path}?mode={'ro' if readonly else 'rwc'}"
        self.conn = sqlite3.connect(uri, uri=True, timeout=30, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=30000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.readonly = readonly
        if not readonly:
            migrate(self.conn)

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
        For origin='import', a differing current digest is only replaced when it appears in
        `prior_digests` (the importing archive's known history for this URN); otherwise a
        conflict is recorded and the current record kept.
        """
        conn = self.conn
        observed_at = observed_at if observed_at is not None else now_ms()
        digest = rec.get("revision_digest") or revision_digest(rec)
        rec["revision_digest"] = digest
        urn = rec["urn"]
        kind = rec["entity"]
        cur = conn.execute("SELECT revision_digest, record FROM records WHERE urn=?", (urn,)).fetchone()
        stored = dict(rec)
        stored["observed_at"] = observed_at
        stored["sync_run"] = sync_run
        if cur is None:
            conn.execute(
                "INSERT INTO records(urn, kind, source, account_scope, revision_digest, observed_at, sync_run, tombstoned, record) VALUES (?,?,?,?,?,?,?,?,?)",
                (urn, kind, rec.get("source"), rec.get("account_scope"), digest, observed_at, sync_run,
                 1 if rec.get("tombstone") else 0, _j(stored)),
            )
            self._add_revision(urn, kind, digest, observed_at, sync_run, None, stored)
            self._index(kind, stored)
            return "inserted"
        if cur["revision_digest"] == digest:
            conn.execute("UPDATE records SET observed_at=?, sync_run=? WHERE urn=?", (observed_at, sync_run, urn))
            return "unchanged"
        # Differing content.
        if origin == "import" and self._is_ancestor(urn, cur["revision_digest"], digest):
            return "older"  # we already moved past this revision
        if origin == "import" and cur["revision_digest"] not in (prior_digests or set()):
            self.add_conflict("content", urn, {
                "current_digest": cur["revision_digest"], "incoming_digest": digest,
                "incoming_observed_at": observed_at, "incoming_sync_run": sync_run,
            })
            # keep the incoming content as a non-current revision for inspection
            self._add_revision(urn, kind, digest, observed_at, sync_run, None, stored, current=False)
            return "conflict"
        conn.execute(
            "UPDATE records SET revision_digest=?, observed_at=?, sync_run=?, tombstoned=?, record=?, source=?, account_scope=? WHERE urn=?",
            (digest, observed_at, sync_run, 1 if rec.get("tombstone") else 0, _j(stored), rec.get("source"), rec.get("account_scope"), urn),
        )
        self._add_revision(urn, kind, digest, observed_at, sync_run, cur["revision_digest"], stored)
        self._index(kind, stored)
        return "updated"

    def _is_ancestor(self, urn: str, current: str, candidate: str) -> bool:
        """True if `candidate` is in the supersedes chain below `current`."""
        seen: set[str] = set()
        d: str | None = current
        while d and d not in seen:
            seen.add(d)
            row = self.conn.execute("SELECT supersedes FROM revisions WHERE entity_urn=? AND revision_digest=?", (urn, d)).fetchone()
            d = row[0] if row else None
            if d == candidate:
                return True
        return False

    def _add_revision(self, urn: str, kind: str, digest: str, observed_at: int, sync_run: str | None,
                      supersedes: str | None, stored: dict[str, Any], *, current: bool = True) -> None:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO revisions(entity_urn, revision_digest, entity_kind, observed_at, sync_run, supersedes, record) VALUES (?,?,?,?,?,?,?)",
            (urn, digest, kind, observed_at, sync_run, supersedes, _j(stored)),
        )
        if kind == "messages" and cur.rowcount == 1 and cur.lastrowid is not None:
            fts.add_revision_text(self.conn, cur.lastrowid, stored.get("text"))

    def _index(self, kind: str, r: dict[str, Any]) -> None:
        c = self.conn
        urn = r["urn"]
        if kind == "messages":
            sent = r.get("sent_at") or r.get("received_at") or {}
            c.execute(
                """INSERT INTO messages_idx(urn, chat_urn, sender_urn, utc_ms, kind, transport, source, account_scope, has_attachments, is_from_me)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(urn) DO UPDATE SET chat_urn=excluded.chat_urn, sender_urn=excluded.sender_urn, utc_ms=excluded.utc_ms,
                     kind=excluded.kind, transport=excluded.transport, source=excluded.source, account_scope=excluded.account_scope,
                     has_attachments=excluded.has_attachments, is_from_me=excluded.is_from_me""",
                (urn, r.get("chat_urn"), r.get("sender_urn"), sent.get("utc_ms"), r["kind"], r["transport"],
                 r["source"], r["account_scope"], 1 if r.get("has_attachments") else 0, 1 if r.get("is_from_me") else 0),
            )
            rowid = c.execute("SELECT id FROM messages_idx WHERE urn=?", (urn,)).fetchone()[0]
            fts.set_message_text(c, rowid, None if r.get("tombstone") else r.get("text"))
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

    def observe(self, entity_urn: str, source: str, account_scope: str, native_table: str,
                native_row_ids: list[Any], native_key: str | None, fingerprint: str,
                adapter_version: str, observed_at: int, sync_run: str | None,
                *, minted: bool = False, present: str = "present") -> None:
        self.conn.execute(
            """INSERT INTO source_observations(entity_urn, source, account_scope, native_table, native_row_ids, native_key, minted, fingerprint, adapter_version, observed_at, sync_run, present)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(entity_urn, source, account_scope, native_table) DO UPDATE SET
                 native_row_ids=excluded.native_row_ids, fingerprint=excluded.fingerprint,
                 adapter_version=excluded.adapter_version, observed_at=excluded.observed_at,
                 sync_run=excluded.sync_run, present=excluded.present""",
            (entity_urn, source, account_scope, native_table, _j(native_row_ids), native_key,
             1 if minted else 0, fingerprint, adapter_version, observed_at, sync_run, present),
        )

    def mark_absent(self, source: str, account_scope: str, native_table: str, seen_before: int,
                    sync_run: str | None, state: str = "absent_from_source") -> int:
        """Mark observations of `native_table` not touched since `seen_before` as absent."""
        cur = self.conn.execute(
            "UPDATE source_observations SET present=?, sync_run=? WHERE source=? AND account_scope=? AND native_table=? AND observed_at<? AND present='present'",
            (state, sync_run, source, account_scope, native_table, seen_before),
        )
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
        row = self.conn.execute("SELECT record FROM records WHERE urn=?", (urn,)).fetchone()
        return json.loads(row[0]) if row else None

    def get_many(self, urns: Iterable[str]) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        urns = [u for u in set(urns) if u]
        for i in range(0, len(urns), 500):
            chunk = urns[i:i + 500]
            q = f"SELECT urn, record FROM records WHERE urn IN ({','.join('?' * len(chunk))})"
            for r in self.conn.execute(q, chunk):
                out[r[0]] = json.loads(r[1])
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

    def observations_of(self, urn: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT source, account_scope, native_table, native_row_ids, native_key, minted, fingerprint, adapter_version, observed_at, sync_run, present FROM source_observations WHERE entity_urn=?",
            (urn,))
        return [dict(r) | {"native_row_ids": json.loads(r["native_row_ids"])} for r in rows]

    def iter_records(self, kind: str, *, source: str | None = None, scope: str | None = None,
                     include_tombstoned: bool = True) -> Iterator[dict[str, Any]]:
        q = "SELECT record FROM records WHERE kind=?"
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
            yield json.loads(r[0])

    def counts(self) -> dict[str, int]:
        return {r[0]: r[1] for r in self.conn.execute("SELECT kind, count(*) FROM records GROUP BY kind ORDER BY kind")}

    def coverage(self, source: str | None = None, scope: str | None = None) -> dict[str, Any]:
        q = "SELECT min(utc_ms), max(utc_ms), count(*) FROM messages_idx WHERE kind='message'"
        args: list[Any] = []
        if source:
            q += " AND source=?"
            args.append(source)
        if scope:
            q += " AND account_scope=?"
            args.append(scope)
        r = self.conn.execute(q, args).fetchone()
        return {"earliest_utc_ms": r[0], "latest_utc_ms": r[1], "messages": r[2]}

    def latest_sync_runs(self) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """SELECT record FROM records r WHERE kind='sync_runs' AND observed_at = (
                 SELECT max(observed_at) FROM records r2 WHERE r2.kind='sync_runs' AND r2.source=r.source AND r2.account_scope=r.account_scope)
               ORDER BY source""")
        return [json.loads(r[0]) for r in rows]

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
