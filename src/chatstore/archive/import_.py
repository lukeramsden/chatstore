"""Import archives into the cache: lineage checks, conflict recording, provenance restore."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..cache.store import Cache, SourceObservation, _j
from ..canonical.records import now_ms
from ..canonical.schemas import ENTITY_ORDER
from ..config import Identity, save_identity
from ..paths import DataDir, blob_path
from . import container
from .container import InvalidArchive, WrongPassword
from .verify import LoadedArchive, SchemaError, load_archive

BUCKETED = ("messages", "message_parts", "events", "attachments")


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
    blobs_failed: int = 0
    conflicts: int = 0

    @property
    def exit_code(self) -> int:
        return {"imported": 0, "already_imported": 0, "superseded": 0, "media_failed": 1, "branch_conflict": 6, "invalid": 7,
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


def _restore_blobs(dd: DataDir, ar: LoadedArchive, result: ImportResult) -> None:
    """Copy content-addressed blobs that are not yet present. Idempotent; safe before the DB transaction
    because a verified blob file is meaningful on its own."""
    for name in ar.blob_members:
        sha = name.rsplit("/", 1)[-1]
        dst = blob_path(dd, sha)
        if dst.exists():
            continue
        try:
            container.extract_blob(ar.tar_path, name, dst, sha)
            result.blobs_restored += 1
        except (OSError, InvalidArchive) as e:
            result.blobs_failed += 1
            if result.blobs_failed <= 3:
                result.problems.append(f"blob {sha[:12]}… not restored: {e}")


def _count_not_restored(dd: DataDir, ar: LoadedArchive) -> int:
    return sum(1 for a in ar.records.get("attachments", [])
               if a.get("availability") == "available" and a.get("blob_sha256") and not blob_path(dd, a["blob_sha256"]).exists())


def _adopt_manifest(dd: DataDir, ident: Identity, m: dict[str, Any]) -> None:
    """Adopt scopes, scope mappings and the export set from the manifest, then persist identity.json.
    Idempotent, so it can run again for archives that are already imported."""
    changed = False
    for sc in m.get("account_scopes", []):
        if sc["source"] not in ident.scopes:
            ident.scopes[sc["source"]] = {"scope": sc["scope"], "created_at": m["created_at"], "label": sc.get("label"),
                                          "origin": f"import:{m['export_id']}"}
            changed = True
    known = {(x.get("from"), x.get("to")) for x in ident.scope_mappings}
    for mp in m.get("scope_mappings", []):
        if (mp.get("from"), mp.get("to")) not in known:
            ident.scope_mappings.append(dict(mp))
            changed = True
    if not ident.export_set:
        ident.export_set = m["export_set"]
        changed = True
    if changed:
        save_identity(dd, ident)


def _apply_records(cache: Cache, ar: LoadedArchive, result: ImportResult) -> None:
    """Records, provenance, retirement and ledger — one transaction. Must run inside write()."""
    m = ar.manifest
    recs = ar.records
    # 1. revision history first: it tells upsert() which current digests belong to this archive's lineage
    prior: dict[str, set[str]] = {}
    for rv in recs.get("revisions", []):
        prior.setdefault(rv["entity_urn"], set()).add(rv["revision_digest"])
        cache.add_revision_row(rv)
    # 2. current records in dependency order
    member_urns: list[str] = []
    outcomes: dict[str, int] = {}
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
            if kind in BUCKETED:
                member_urns.append(rec["urn"])
    # 3. provenance (newer observations only)
    cache.merge_observations(
        SourceObservation(o["entity_urn"], o["source"], o["account_scope"], o["native_table"], o["native_row_ids"], o.get("native_key"),
                          o["fingerprint"], o["adapter_version"], o["observed_at"], o.get("sync_run"), bool(o.get("minted")), o["present"])
        for o in recs.get("source_observations", []))
    # 4. retire records of superseded revisions that this revision no longer contains
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
    # 5. ledger
    cache.conn.execute(
        "INSERT INTO imports(export_id, export_set, kind, bucket_label, start_utc_ms, end_utc_ms, revision, lineage, imported_at, superseded_by, manifest) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (m["export_id"], m["export_set"], m["kind"], ar.label, (m.get("bucket") or {}).get("start_utc_ms"),
         (m.get("bucket") or {}).get("end_utc_ms"), m["revision"], json.dumps(m["lineage"]), now_ms(), None, _j(m)))
    cache.conn.executemany("INSERT OR IGNORE INTO bucket_membership(export_id, urn) VALUES (?,?)",
                           [(m["export_id"], u) for u in member_urns])
    # 6. references this archive could not resolve yet
    unresolved = unresolved_context = 0
    for st in recs.get("stubs", []):
        if cache.conn.execute("SELECT 1 FROM records WHERE urn=?", (st["urn"],)).fetchone() is None:
            unresolved += 1
            unresolved_context += st.get("entity") in ("chats", "identities")
    if unresolved_context and m.get("context") == "minimal":
        result.problems.append(f"{unresolved_context} chat/identity reference(s) unresolved: this archive carries "
                               "minimal context; import the catalogue archive to label them")
    result.outcomes = outcomes
    result.retired = retired
    result.unresolved_refs = unresolved
    result.conflicts = outcomes.get("conflict", 0)


def import_archive(cache: Cache, dd: DataDir, ident: Identity, path: Path, password: str, *,
                   staging: Path | None = None, restore_media: bool = True) -> ImportResult:
    """Import one archive. Completion means: records committed, identity.json updated, and every blob
    the archive carries present under blobs/. Re-running on an imported archive restores missing blobs."""
    work = Path(tempfile.mkdtemp(prefix="chatstore-import-", dir=str(staging) if staging else None))
    os.chmod(work, 0o700)
    try:
        try:
            ar = load_archive(path, password, work)
        except WrongPassword as e:
            return ImportResult(str(path), "wrong_password", problems=[str(e)])
        except SchemaError as e:
            return ImportResult(str(path), "schema_error", problems=e.problems, counts=e.counts)
        except InvalidArchive as e:
            return ImportResult(str(path), "invalid", problems=[str(e)])
        m = ar.manifest
        result = ImportResult(str(path), "imported", export_id=m["export_id"], kind=m["kind"], label=ar.label,
                              revision=m["revision"], counts=ar.counts)
        decision, notes = _lineage_check(cache, m)
        if decision == "already_imported":
            result.action = decision
            _adopt_manifest(dd, ident, m)
            if restore_media:
                _restore_blobs(dd, ar, result)
            result.media_not_restored = _count_not_restored(dd, ar)
            return result
        if decision != "proceed":
            result.action = decision
            result.problems = notes
            if decision == "branch_conflict":
                with cache.write():
                    cache.add_conflict("archive_branch", None, {"export_id": m["export_id"], "export_set": m["export_set"],
                                                                "kind": m["kind"], "label": ar.label, "notes": notes,
                                                                "path": str(path)})
            return result
        result.problems.extend(notes)
        # Blobs first: content-addressed files are harmless on their own, and a failure here must not
        # leave the ledger claiming the archive is complete.
        if restore_media:
            _restore_blobs(dd, ar, result)
            if result.blobs_failed:
                result.action = "media_failed"
                result.problems.append("archive not recorded as imported because media restore failed; re-run to retry")
                return result
        with cache.bulk(), cache.write():
            _apply_records(cache, ar, result)
        _adopt_manifest(dd, ident, m)
        result.media_not_restored = _count_not_restored(dd, ar)
        return result
    finally:
        shutil.rmtree(work, ignore_errors=True)
