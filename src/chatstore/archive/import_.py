"""Import archives into the cache: lineage checks, conflict recording, provenance restore."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..cache import fts
from ..cache.store import Cache, _j
from ..canonical.records import now_ms
from ..canonical.schemas import ENTITY_ORDER
from ..config import Identity
from ..paths import DataDir, blob_path
from . import container
from .container import InvalidArchive, WrongPassword
from .verify import check_records, decrypt_and_validate


@dataclass
class ImportResult:
    path: str
    action: str  # imported | already_imported | superseded | branch_conflict | invalid | wrong_password | schema_error
    export_id: str | None = None
    kind: str | None = None
    label: str | None = None
    revision: int | None = None
    problems: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    outcomes: dict[str, int] = field(default_factory=dict)  # inserted/updated/unchanged/older/conflict
    retired: int = 0
    unresolved_refs: int = 0
    blobs_restored: int = 0
    media_not_restored: int = 0
    conflicts: int = 0

    @property
    def exit_code(self) -> int:
        return {"imported": 0, "already_imported": 0, "superseded": 0, "branch_conflict": 6, "invalid": 7,
                "wrong_password": 7, "schema_error": 7}[self.action]


def order_paths(paths: list[Path]) -> list[Path]:
    """Catalogues first, then buckets by label, then revision (parsed from the filename)."""
    def key(p: Path) -> tuple[int, str, str]:
        n = p.name
        return (0 if "-catalogue-" in n else 1, n.rsplit("-r", 1)[0], n)
    return sorted(paths, key=key)


def _lineage_check(cache: Cache, m: dict[str, Any]) -> tuple[str, list[str]]:
    """Return (decision, notes): proceed | already_imported | superseded | branch_conflict."""
    export_id = m["export_id"]
    if cache.conn.execute("SELECT 1 FROM imports WHERE export_id=?", (export_id,)).fetchone():
        return "already_imported", []
    rows = cache.conn.execute(
        "SELECT export_id, lineage, superseded_by FROM imports WHERE export_set=? AND kind=? AND bucket_label=?",
        (m["export_set"], m["kind"], (m.get("bucket") or {}).get("label") or m["kind"])).fetchall()
    my_lineage = list(m.get("lineage") or [export_id])
    from ..curation import branch_accepted
    if branch_accepted(cache, export_id):
        return "proceed", ["branch previously accepted via `chatstore conflicts resolve --accept`"]
    for r in rows:
        other_lineage = json.loads(r["lineage"])
        if export_id in other_lineage:
            return "superseded", [f"already imported {r['export_id']} which supersedes this archive"]
        if r["export_id"] not in my_lineage:
            return "branch_conflict", [f"imported archive {r['export_id']} is not in this archive's lineage"]
    return "proceed", []


def _iter_jsonl(data: bytes) -> list[dict[str, Any]]:
    return [json.loads(line) for line in data.splitlines() if line.strip()]


def import_archive(cache: Cache, dd: DataDir, ident: Identity, path: Path, password: str, *,
                   staging: Path | None = None, restore_media: bool = True) -> ImportResult:
    work = Path(tempfile.mkdtemp(prefix="chatstore-import-", dir=str(staging) if staging else None))
    os.chmod(work, 0o700)
    try:
        try:
            payload, tar_path = decrypt_and_validate(path, password, work)
        except WrongPassword as e:
            return ImportResult(str(path), "wrong_password", problems=[str(e)])
        except InvalidArchive as e:
            return ImportResult(str(path), "invalid", problems=[str(e)])
        m = payload.manifest
        counts, problems, _schema_errors = check_records(payload)
        label = (m.get("bucket") or {}).get("label") or m["kind"]
        base = ImportResult(str(path), "imported", export_id=m["export_id"], kind=m["kind"], label=label,
                            revision=m.get("revision"), counts=counts)
        if problems:
            base.action = "schema_error"
            base.problems = problems
            return base
        if m.get("id_version") and m["id_version"] != "chatstore-id-v1":
            base.action = "invalid"
            base.problems = [f"unsupported id_version {m['id_version']}"]
            return base
        decision, notes = _lineage_check(cache, m)
        if decision != "proceed":
            base.action = decision
            base.problems = notes
            if decision == "branch_conflict":
                with cache.write():
                    cache.add_conflict("archive_branch", None, {"export_id": m["export_id"], "export_set": m["export_set"],
                                                                "kind": m["kind"], "label": label, "notes": notes,
                                                                "path": str(path)})
            return base
        recs: dict[str, list[dict[str, Any]]] = {}
        for name, data in payload.members.items():
            if name.startswith("records/"):
                recs[name.removeprefix("records/").removesuffix(".jsonl")] = _iter_jsonl(data)
        outcomes: dict[str, int] = {}
        with cache.write():
            # 1. revisions first so supersedes chains exist for upsert()'s ancestry check
            prior: dict[str, set[str]] = {}
            for rv in recs.get("revisions", []):
                prior.setdefault(rv["entity_urn"], set()).add(rv["revision_digest"])
                cur = cache.conn.execute(
                    "INSERT OR IGNORE INTO revisions(entity_urn, revision_digest, entity_kind, observed_at, sync_run, supersedes, record) VALUES (?,?,?,?,?,?,?)",
                    (rv["entity_urn"], rv["revision_digest"], rv["entity_kind"], rv["observed_at"], rv.get("sync_run"),
                     rv.get("supersedes"), _j(rv["record"])))
                if rv["entity_kind"] == "messages" and cur.rowcount == 1 and cur.lastrowid is not None:
                    fts.add_revision_text(cache.conn, cur.lastrowid, rv["record"].get("text"))
            # 2. records in dependency order
            member_urns: list[str] = []
            for kind in ENTITY_ORDER:
                if kind in ("revisions", "source_observations"):
                    continue
                for rec in recs.get(kind, []):
                    stored = dict(rec)
                    observed_at = int(stored.pop("observed_at", None) or m["created_at"])
                    sync_run = stored.pop("sync_run", None)
                    stored.pop("iso", None)
                    out = cache.upsert(stored, observed_at=observed_at, sync_run=sync_run,
                                       prior_digests=prior.get(rec["urn"], set()), origin="import")
                    outcomes[out] = outcomes.get(out, 0) + 1
                    if kind in ("messages", "message_parts", "events", "attachments"):
                        member_urns.append(rec["urn"])
            # 3. provenance
            for o in recs.get("source_observations", []):
                cache.conn.execute(
                    """INSERT INTO source_observations(entity_urn, source, account_scope, native_table, native_row_ids, native_key, minted, fingerprint, adapter_version, observed_at, sync_run, present)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                       ON CONFLICT(entity_urn, source, account_scope, native_table) DO UPDATE SET
                         native_row_ids=excluded.native_row_ids, fingerprint=excluded.fingerprint, adapter_version=excluded.adapter_version,
                         observed_at=excluded.observed_at, sync_run=excluded.sync_run, present=excluded.present
                       WHERE excluded.observed_at >= source_observations.observed_at""",
                    (o["entity_urn"], o["source"], o["account_scope"], o["native_table"], _j(o["native_row_ids"]), o.get("native_key"),
                     1 if o.get("minted") else 0, o["fingerprint"], o["adapter_version"], o["observed_at"], o.get("sync_run"), o["present"]))
            # 4. scopes and mappings from the manifest
            for sc in m.get("account_scopes", []):
                if sc["source"] not in ident.scopes:
                    ident.scopes[sc["source"]] = {"scope": sc["scope"], "created_at": m["created_at"], "label": sc.get("label"),
                                                  "origin": f"import:{m['export_id']}"}
            known = {(x.get("from"), x.get("to")) for x in ident.scope_mappings}
            for mp in m.get("scope_mappings", []):
                if (mp.get("from"), mp.get("to")) not in known:
                    ident.scope_mappings.append(dict(mp))
            if not ident.export_set:
                ident.export_set = m["export_set"]
            # 5. retire records of the superseded revision that are absent now
            retired = 0
            new_set = set(member_urns)
            for prev_id in m.get("supersedes", []):
                for (u,) in cache.conn.execute("SELECT urn FROM bucket_membership WHERE export_id=?", (prev_id,)).fetchall():
                    if u in new_set:
                        continue
                    existing = cache.get(u)
                    if existing and not existing.get("tombstone"):
                        tomb = {k: v for k, v in existing.items() if k not in ("observed_at", "sync_run", "iso", "revision_digest")}
                        tomb["tombstone"] = {"reason": "retired_by_archive_revision", "export_id": m["export_id"]}
                        cache.upsert(tomb, observed_at=m["created_at"], sync_run=None)
                        retired += 1
                cache.conn.execute("UPDATE imports SET superseded_by=? WHERE export_id=?", (m["export_id"], prev_id))
            # 6. ledger
            cache.conn.execute(
                "INSERT INTO imports(export_id, export_set, kind, bucket_label, start_utc_ms, end_utc_ms, revision, lineage, imported_at, superseded_by, manifest) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (m["export_id"], m["export_set"], m["kind"], label, (m.get("bucket") or {}).get("start_utc_ms"),
                 (m.get("bucket") or {}).get("end_utc_ms"), m["revision"], json.dumps(m.get("lineage") or [m["export_id"]]),
                 now_ms(), None, _j(m)))
            cache.conn.executemany("INSERT OR IGNORE INTO bucket_membership(export_id, urn) VALUES (?,?)",
                                   [(m["export_id"], u) for u in member_urns])
            unresolved = 0
            for st in recs.get("stubs", []):
                if cache.conn.execute("SELECT 1 FROM records WHERE urn=?", (st["urn"],)).fetchone() is None:
                    unresolved += 1
        # 7. blobs (outside the transaction: file copies)
        restored = 0
        if restore_media:
            for name in payload.blob_members:
                sha = name.rsplit("/", 1)[-1]
                dst = blob_path(dd, sha)
                if dst.exists():
                    continue
                container.extract_blob(tar_path, name, dst, sha)
                restored += 1
        not_restored = 0
        for a in recs.get("attachments", []):
            if a.get("availability") == "available" and a.get("blob_sha256") and not blob_path(dd, a["blob_sha256"]).exists():
                not_restored += 1
        base.media_not_restored = not_restored
        base.outcomes = outcomes
        base.retired = retired
        base.unresolved_refs = unresolved
        base.blobs_restored = restored
        base.conflicts = outcomes.get("conflict", 0)
        return base
    finally:
        shutil.rmtree(work, ignore_errors=True)
