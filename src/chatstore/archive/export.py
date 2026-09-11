"""Export the cache into encrypted, immutable, monthly archives (chatstore-archive-v1)."""

from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..cache.store import Cache
from ..canonical.records import now_ms
from ..config import Config, Identity
from ..media import locate_blob
from ..paths import DataDir
from . import container
from . import manifest as M
from .container import DEFAULT_LIMITS, TarBuilder

Progress = Callable[[str], None]


# Records a bucket carries only so it can be read on its own; the catalogue is their authority.
CONTEXT_KINDS = frozenset({"accounts", "identities", "aliases", "chats", "chat_memberships", "sync_runs"})
CONTEXT_MODES = ("full", "minimal")


@dataclass
class BucketPlan:
    bucket: M.Bucket
    records: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: {k: [] for k in M.RECORD_ORDER})
    stubs: list[dict[str, str]] = field(default_factory=list)
    blobs: list[dict[str, Any]] = field(default_factory=list)  # attachment records with availability=available
    sync_runs: set[str] = field(default_factory=set)
    content_digest: str = ""
    context: str = "full"

    def pairs(self) -> list[tuple[str, str]]:
        """(urn, digest) pairs that define this archive's content. Buckets exclude context records,
        so renaming a chat does not create a new revision of every month; the catalogue covers them."""
        out: list[tuple[str, str]] = []
        for k, recs in self.records.items():
            if k in ("revisions", "source_observations"):
                continue
            if self.bucket.kind != "catalogue" and k in CONTEXT_KINDS:
                continue
            out.extend((r["urn"], r["revision_digest"]) for r in recs)
        return out


@dataclass
class ExportResult:
    kind: str
    label: str
    action: str  # written | unchanged | skipped_empty
    path: str | None = None
    export_id: str | None = None
    revision: int | None = None
    supersedes: list[str] = field(default_factory=list)
    counts: dict[str, int] = field(default_factory=dict)
    media: dict[str, int] = field(default_factory=dict)
    coverage: dict[str, Any] = field(default_factory=dict)


# ---- collection -----------------------------------------------------------------------------------

def _rows(cache: Cache, q: str, args: Iterable[Any] = ()) -> list[Any]:
    return list(cache.conn.execute(q, list(args)))


def _records(cache: Cache, urns: Iterable[str]) -> list[dict[str, Any]]:
    got = cache.get_many(urns)
    return [got[u] for u in sorted(got)]


def _revisions_and_observations(cache: Cache, plan: BucketPlan, urns: set[str]) -> None:
    urn_list = sorted(urns)
    for i in range(0, len(urn_list), 500):
        chunk = urn_list[i:i + 500]
        ph = ",".join("?" * len(chunk))
        for r in cache.conn.execute(
                f"SELECT entity_urn, revision_digest, entity_kind, observed_at, sync_run, supersedes, record FROM revisions WHERE entity_urn IN ({ph}) ORDER BY entity_urn, observed_at, revision_digest", chunk):
            plan.records["revisions"].append({
                "entity_urn": r[0], "revision_digest": r[1], "entity_kind": r[2], "observed_at": r[3],
                "sync_run": r[4], "supersedes": r[5], "record": json.loads(r[6])})
            if r[4]:
                plan.sync_runs.add(r[4])
        for r in cache.conn.execute(
                f"SELECT entity_urn, source, account_scope, native_table, native_row_ids, native_key, minted, fingerprint, adapter_version, observed_at, sync_run, present FROM source_observations WHERE entity_urn IN ({ph}) ORDER BY entity_urn, native_table", chunk):
            plan.records["source_observations"].append({
                "entity_urn": r[0], "source": r[1], "account_scope": r[2], "native_table": r[3],
                "native_row_ids": json.loads(r[4]), "native_key": r[5], "minted": bool(r[6]), "fingerprint": r[7],
                "adapter_version": r[8], "observed_at": r[9], "sync_run": r[10], "present": r[11]})
            if r[10]:
                plan.sync_runs.add(r[10])


def collect_bucket(cache: Cache, bucket: M.Bucket, *, context: str = "full") -> BucketPlan:
    """context='full': carry chats/identities/memberships/aliases needed to read the bucket alone.
    context='minimal': carry only stubs for them (smaller archives; requires the catalogue on restore)."""
    if context not in CONTEXT_MODES:
        raise ValueError(f"context must be one of {CONTEXT_MODES}")
    plan = BucketPlan(bucket, context=context)
    if bucket.kind == "undated":
        msg_urns = [r[0] for r in _rows(cache, "SELECT urn FROM messages_idx WHERE utc_ms IS NULL ORDER BY urn")]
        ev_rows = _rows(cache, "SELECT urn FROM events_idx WHERE at_ms IS NULL AND (target_urn IS NULL OR target_urn NOT IN (SELECT urn FROM messages_idx)) ORDER BY urn")
    else:
        s, e = bucket.start_utc_ms, bucket.end_utc_ms
        msg_urns = [r[0] for r in _rows(cache, "SELECT urn FROM messages_idx WHERE utc_ms >= ? AND utc_ms < ? ORDER BY urn", (s, e))]
        ev_rows = _rows(cache, "SELECT urn FROM events_idx WHERE at_ms >= ? AND at_ms < ? AND (target_urn IS NULL OR target_urn NOT IN (SELECT urn FROM messages_idx)) ORDER BY urn", (s, e))
    msg_set = set(msg_urns)
    messages = _records(cache, msg_urns)
    plan.records["messages"] = messages
    # dependants travel with the message
    dep: dict[str, list[str]] = {"message_parts": [], "attachments": [], "events": []}
    for i in range(0, len(msg_urns), 500):
        chunk = msg_urns[i:i + 500]
        ph = ",".join("?" * len(chunk))
        dep["message_parts"] += [r[0] for r in cache.conn.execute(f"SELECT urn FROM parts_idx WHERE message_urn IN ({ph})", chunk)]
        dep["attachments"] += [r[0] for r in cache.conn.execute(f"SELECT urn FROM attachments_idx WHERE message_urn IN ({ph})", chunk)]
        dep["events"] += [r[0] for r in cache.conn.execute(f"SELECT urn FROM events_idx WHERE target_urn IN ({ph})", chunk)]
    dep["events"] += [r[0] for r in ev_rows]
    for k, urns in dep.items():
        plan.records[k] = _records(cache, urns)
    # context: chats, identities, memberships, aliases
    chat_urns: set[str] = set()
    ident_urns: set[str] = set()
    refs: set[str] = set()
    for m in messages:
        if m.get("chat_urn"):
            chat_urns.add(m["chat_urn"])
        if m.get("sender_urn"):
            ident_urns.add(m["sender_urn"])
        if m.get("reply_to_urn"):
            refs.add(m["reply_to_urn"])
    for ev in plan.records["events"]:
        if ev.get("actor_urn"):
            ident_urns.add(ev["actor_urn"])
        t = ev.get("target_urn")
        if t and t not in msg_set:
            refs.add(t)
        for k in ("subject_urn", "identity_urn"):
            if ev.get(k):
                ident_urns.add(ev[k])
    for a in plan.records["attachments"]:
        if a.get("availability") == "available" and a.get("blob_sha256"):
            plan.blobs.append(a)
    # resolve refs: chats become context, unknown/cross-bucket messages become stubs
    ref_recs = cache.get_many(refs)
    for u in sorted(refs):
        r = ref_recs.get(u)
        if r is None:
            plan.stubs.append({"urn": u, "entity": "unknown"})
        elif r["entity"] == "chats":
            chat_urns.add(u)
        elif r["entity"] == "identities":
            ident_urns.add(u)
        else:
            plan.stubs.append({"urn": u, "entity": r["entity"]})
    if context == "minimal":
        plan.stubs += [{"urn": u, "entity": "chats"} for u in sorted(chat_urns)]
        plan.stubs += [{"urn": u, "entity": "identities"} for u in sorted(ident_urns - {""})]
        plan.records["accounts"] = list(cache.iter_records("accounts"))
        shas = {a["blob_sha256"] for a in plan.blobs}
        plan.records["blobs"] = [b for b in cache.iter_records("blobs") if b.get("sha256") in shas]
        prov = msg_set | set(dep["message_parts"]) | set(dep["attachments"]) | set(dep["events"])
        _revisions_and_observations(cache, plan, prov)
        plan.records["sync_runs"] = _records(cache, plan.sync_runs)
        plan.content_digest = M.content_digest(plan.pairs())
        return plan
    memberships: list[str] = []
    for i in range(0, len(chat_urns), 500):
        chunk = sorted(chat_urns)[i:i + 500]
        ph = ",".join("?" * len(chunk))
        for r in cache.conn.execute(f"SELECT urn, identity_urn FROM memberships_idx WHERE chat_urn IN ({ph})", chunk):
            memberships.append(r[0])
            ident_urns.add(r[1])
    ident_urns.discard("")
    aliases: list[str] = []
    for i in range(0, len(ident_urns), 500):
        chunk = sorted(ident_urns)[i:i + 500]
        ph = ",".join("?" * len(chunk))
        for r in cache.conn.execute(f"SELECT urn, old_urn, new_urn FROM aliases_idx WHERE old_urn IN ({ph}) OR new_urn IN ({ph})", chunk + chunk):
            aliases.append(r[0])
    alias_recs = _records(cache, aliases)
    for a in alias_recs:
        ident_urns.add(a["old_urn"])
        ident_urns.add(a["new_urn"])
    plan.records["chats"] = _records(cache, chat_urns)
    plan.records["identities"] = _records(cache, ident_urns)
    plan.records["chat_memberships"] = _records(cache, memberships)
    plan.records["aliases"] = alias_recs
    plan.records["accounts"] = list(cache.iter_records("accounts"))
    shas = {a["blob_sha256"] for a in plan.blobs}
    plan.records["blobs"] = [b for b in cache.iter_records("blobs") if b.get("sha256") in shas]
    # provenance for the bucketed records (messages, parts, attachments, events)
    prov = msg_set | set(dep["message_parts"]) | set(dep["attachments"]) | set(dep["events"])
    _revisions_and_observations(cache, plan, prov)
    plan.records["sync_runs"] = _records(cache, plan.sync_runs)
    plan.content_digest = M.content_digest(plan.pairs())
    return plan


def collect_catalogue(cache: Cache) -> BucketPlan:
    plan = BucketPlan(M.Bucket("catalogue", "catalogue"))
    for k in ("accounts", "identities", "aliases", "chats", "chat_memberships", "people", "identity_links"):
        plan.records[k] = list(cache.iter_records(k))
    prov = {r["urn"] for k in ("chats", "identities", "aliases", "chat_memberships", "people", "identity_links") for r in plan.records[k]}
    _revisions_and_observations(cache, plan, prov)
    plan.records["sync_runs"] = list(cache.iter_records("sync_runs"))
    plan.content_digest = M.content_digest(plan.pairs())
    return plan


def plan_buckets(cache: Cache, since_ms: int | None, until_ms: int | None) -> list[M.Bucket]:
    """Monthly buckets covering current messages *and* every bucket exported before. A bucket that has
    become empty must still be planned so a superseding (empty) revision retires its old contents."""
    if since_ms is not None and until_ms is not None:
        return [M.Bucket("range", M.range_label(since_ms, until_ms), since_ms, until_ms)]
    out = _current_buckets(cache, since_ms, until_ms)
    have = {b.label for b in out}
    for r in cache.conn.execute("SELECT DISTINCT kind, bucket_label, start_utc_ms, end_utc_ms FROM exports WHERE kind IN ('bucket', 'undated')"):
        if r["bucket_label"] in have:
            continue
        if since_ms is not None and r["end_utc_ms"] is not None and r["end_utc_ms"] <= since_ms:
            continue
        if until_ms is not None and r["start_utc_ms"] is not None and r["start_utc_ms"] >= until_ms:
            continue
        out.append(M.Bucket(r["kind"], r["bucket_label"], r["start_utc_ms"], r["end_utc_ms"]))
        have.add(r["bucket_label"])
    out.sort(key=lambda b: (b.start_utc_ms is None, b.start_utc_ms or 0, b.label))
    return out


def _current_buckets(cache: Cache, since_ms: int | None, until_ms: int | None) -> list[M.Bucket]:
    r = cache.conn.execute("SELECT min(utc_ms), max(utc_ms) FROM messages_idx WHERE utc_ms IS NOT NULL").fetchone()
    out: list[M.Bucket] = []
    if r and r[0] is not None:
        lo, hi = r[0], r[1]
        if since_ms is not None:
            lo = max(lo, since_ms)
        if until_ms is not None:
            hi = min(hi, until_ms - 1)
        if lo <= hi:
            out = M.monthly_buckets(lo, hi)
    if since_ms is None and until_ms is None:
        n = cache.conn.execute("SELECT count(*) FROM messages_idx WHERE utc_ms IS NULL").fetchone()[0]
        if n:
            out.append(M.Bucket("undated", "undated"))
    return out


# ---- writing --------------------------------------------------------------------------------------

def _jsonl(recs: list[dict[str, Any]]) -> bytes:
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n" for r in recs).encode("utf-8")


def _previous(cache: Cache, export_set: str, bucket: M.Bucket) -> Any:
    return cache.conn.execute(
        "SELECT export_id, revision, content_digest, lineage FROM exports WHERE export_set=? AND kind=? AND bucket_label=? ORDER BY revision DESC LIMIT 1",
        (export_set, bucket.kind, bucket.label)).fetchone()


def write_archive(cache: Cache, dd: DataDir, cfg: Config, ident: Identity, plan: BucketPlan, *,
                  output: Path, password: str, media: str, force: bool, adapter_versions: dict[str, str],
                  progress: Progress | None = None) -> ExportResult:
    bucket = plan.bucket
    export_set, _ = ident.ensure_export_set()
    total = sum(len(v) for k, v in plan.records.items() if k not in ("revisions", "source_observations", "accounts", "sync_runs"))
    prev = _previous(cache, export_set, bucket)
    if total == 0 and prev is None:
        return ExportResult(bucket.kind, bucket.label, "skipped_empty")
    # total == 0 with a previous revision: write an empty superseding revision so restores retire the old contents.
    media_counts = {"included_blobs": 0, "missing": 0, "not_downloaded": 0, "not_exported": 0}
    for a in plan.records["attachments"]:
        av = a.get("availability")
        if av == "missing":
            media_counts["missing"] += 1
        elif av == "not_downloaded":
            media_counts["not_downloaded"] += 1
    # content digest includes the media policy so text vs media exports are distinct revisions
    content = plan.content_digest + f"|media={media}" + (f"|context={plan.context}" if plan.context != "full" else "")
    if prev and prev["content_digest"] == content and not force:
        return ExportResult(bucket.kind, bucket.label, "unchanged", export_id=prev["export_id"], revision=prev["revision"])
    revision = (prev["revision"] + 1) if prev else 1
    supersedes = [prev["export_id"]] if prev else []
    lineage = (json.loads(prev["lineage"]) if prev else [])
    export_id = f"urn:uuid:{uuid.uuid4()}"
    lineage = lineage + [export_id]
    created = now_ms()
    staging = dd.staging / export_id.removeprefix("urn:uuid:")
    staging.mkdir(parents=True, exist_ok=False)
    os.chmod(staging, 0o700)
    try:
        tar_path = staging / "payload.tar"
        tb = TarBuilder(tar_path)
        members: list[str] = []
        counts: dict[str, int] = {}
        # manifest placeholder: we need payload digest of the TAR *excluding* manifest? No — the manifest
        # is the first member, so build the manifest first with digest over records, then write.
        record_bytes: dict[str, bytes] = {}
        for k in M.RECORD_ORDER:
            if k in M.CATALOGUE_ONLY and bucket.kind != "catalogue":
                continue
            recs = plan.records.get(k, [])
            if k == "attachments" and media == "text":
                # records are exported verbatim (availability is a statement about the source);
                # the manifest records how many blobs existed but were not shipped.
                media_counts["not_exported"] = sum(1 for r in recs if r.get("availability") == "available")
            record_bytes[f"records/{k}.jsonl"] = _jsonl(recs)
            counts[k] = len(recs)
        if plan.stubs:
            record_bytes["records/stubs.jsonl"] = _jsonl(plan.stubs)
            counts["stubs"] = len(plan.stubs)
        blob_files: list[tuple[str, Path, str, int | None]] = []
        if media == "available-media":
            seen: set[str] = set()
            for a in plan.blobs:
                sha = a["blob_sha256"]
                if sha in seen:
                    continue
                p = locate_blob(dd, cfg, a)
                if p is None:
                    media_counts["missing"] += 1
                    continue
                seen.add(sha)
                blob_files.append((f"blobs/sha256/{sha[:2]}/{sha}", p, sha, a.get("declared_size")))
            media_counts["included_blobs"] = len(blob_files)
        counts["blob_files"] = len(blob_files)
        members = list(record_bytes) + [b[0] for b in blob_files]
        obs = plan.records["source_observations"]
        observation = {
            "earliest_observed_at": min((o["observed_at"] for o in obs), default=None),
            "latest_observed_at": max((o["observed_at"] for o in obs), default=None),
            "sync_runs": sorted(plan.sync_runs),
        }
        notes = []
        partial_runs = [r for r in plan.records["sync_runs"] if r.get("status") != "complete"]
        if partial_runs:
            notes.append(f"{len(partial_runs)} contributing sync run(s) were not complete")
        coverage = {"status": "partial" if partial_runs else "complete", "notes": notes}
        # The payload digest covers the TAR; the manifest inside cannot contain its own TAR digest.
        # payload_digest therefore covers the TAR *without* manifest.json (all other members), computed
        # from member checksums in order; see specs/archive-format.md.
        manifest = M.build_manifest(
            export_id=export_id, export_set=export_set, created_at=created, bucket=bucket, revision=revision,
            supersedes=supersedes, lineage=lineage,
            account_scopes=[{"scope": v["scope"], "source": s, "label": v.get("label")} for s, v in sorted(ident.scopes.items())],
            scope_mappings=list(ident.scope_mappings), observation=observation, coverage=coverage,
            adapter_versions=adapter_versions, counts=counts, attachment_policy=media, media=media_counts,
            payload_digest="", limits=dict(DEFAULT_LIMITS), members=members, content_digest=content)
        manifest["context"] = plan.context
        # write members first into the TAR (manifest first requires digest; so compute checksums first)
        if progress:
            progress(f"{bucket.label}: writing {sum(counts.values())} records, {len(blob_files)} blobs")
        # Two-pass: build the body TAR to obtain checksums, then the final TAR with manifest first.
        body = TarBuilder(staging / "body.tar")
        for name, data in record_bytes.items():
            body.add_bytes(name, data)
        for name, p, sha, size in blob_files:
            body.add_file(name, p, expected_sha256=sha, expected_size=size)
        body.finish()
        checksums = dict(body.checksums)
        manifest["payload_digest"] = "sha256:" + container.members_digest(checksums)
        tb.add_bytes("manifest.json", M.manifest_bytes(manifest))
        tb.add_bytes("checksums.json", json.dumps(checksums, sort_keys=True, indent=1).encode())
        for name, data in record_bytes.items():
            tb.add_bytes(name, data)
        for name, p, sha, size in blob_files:
            tb.add_file(name, p, expected_sha256=sha, expected_size=size)
        tb.finish()
        (staging / "body.tar").unlink()
        if tar_path.stat().st_size > DEFAULT_LIMITS["max_total_bytes"]:
            raise RuntimeError(f"{bucket.label}: payload exceeds the {DEFAULT_LIMITS['max_total_bytes']} byte limit; "
                               "use --media text or a narrower --since/--until range")
        output.mkdir(parents=True, exist_ok=True)
        dst = output / M.filename(export_set, bucket, revision)
        if dst.exists():
            raise FileExistsError(f"refusing to overwrite existing archive {dst.name}")
        container.write_encrypted_zip(dst, tar_path, password)
        # verification pass
        from .verify import verify_archive
        v = verify_archive(dst, password, staging=staging)
        if not v.ok or v.manifest is None or v.manifest.get("export_id") != export_id:
            dst.unlink(missing_ok=True)
            raise RuntimeError("verification of freshly written archive failed: " + "; ".join(v.problems))
        with cache.write():
            cache.conn.execute(
                "INSERT INTO exports(export_id, export_set, kind, bucket_label, start_utc_ms, end_utc_ms, revision, content_digest, lineage, path, created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (export_id, export_set, bucket.kind, bucket.label, bucket.start_utc_ms, bucket.end_utc_ms, revision, content,
                 json.dumps(lineage), str(dst), created))
        return ExportResult(bucket.kind, bucket.label, "written", path=str(dst), export_id=export_id, revision=revision,
                            supersedes=supersedes, counts=counts, media=media_counts, coverage=coverage)
    finally:
        shutil.rmtree(staging, ignore_errors=True)
