"""Sync orchestration: diagnose → snapshot → extract → transactional import → FTS → report."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from .adapters.base import Emit, ExtractStats, IncompatibleSource, SourceAdapter
from .adapters.snapshot import PermissionProblem, SnapshotError
from .cache import Cache, SourceObservation
from .canonical.records import now_ms, record, restamp
from .config import Config, Identity
from .paths import DataDir

BATCH = 5000


@dataclass
class SyncOutcome:
    run: dict[str, Any]
    exit_code: int  # 0 complete, 3 permission, 4 incompatible, 5 partial


def run_sync(adapter: SourceAdapter, dd: DataDir, cfg: Config, ident: Identity, cache: Cache, *,
             full: bool = False, progress: Callable[[str], None] | None = None) -> SyncOutcome:
    scope = ident.scope_for(adapter.source)
    run_urn = uuid.uuid4().urn
    started = now_ms()
    stats = ExtractStats()
    written: dict[str, int] = {"inserted": 0, "updated": 0, "unchanged": 0}
    mode = "initial"
    prior = cache.get_checkpoint(adapter.source, scope, "adapter")
    if prior is not None:
        mode = "full_reconcile" if full else "incremental"
    run: dict[str, Any] = {
        "started_at": started, "finished_at": None, "status": "running", "mode": mode, "snapshot": None,
        "checkpoints": None, "counts": {}, "coverage": None, "errors": [], "adapter_version": adapter.version,
        "parser_version": None,
    }

    def save_run(status: str) -> None:
        run["status"] = status
        run["finished_at"] = now_ms()
        # `written` is measured at the cache and means the same for every adapter: how many emitted
        # records were new, changed, or identical to what was already stored. The other keys are the
        # adapter's own tallies of what it emitted (which may be a full re-read on incremental runs).
        run["counts"] = dict(stats.counts) | {"unsupported": dict(stats.unsupported), "written": dict(written)}
        run["errors"] = list(stats.errors)
        run["coverage"] = cache.coverage(adapter.source, scope) | {"notes": list(stats.notes)}
        run["checkpoints"] = dict(stats.checkpoints) or None
        rec = record("sync_runs", run_urn, adapter.source, scope, **run)
        with cache.write():
            cache.upsert(rec, observed_at=run["finished_at"], sync_run=run_urn)

    say = progress or (lambda s: None)
    say("diagnosing source")
    diag = adapter.diagnose(cfg)
    if not diag.readable:
        stats.error("permission", diag.permission_hint)
        save_run("failed")
        return SyncOutcome(run, 3)
    if not diag.schema_ok:
        stats.error("incompatible_schema", "; ".join(diag.problems))
        save_run("failed")
        return SyncOutcome(run, 4)

    say("snapshotting")
    try:
        snap = adapter.snapshot(cfg, dd.staging)
    except PermissionProblem as e:
        stats.error("permission", str(e))
        save_run("failed")
        return SyncOutcome(run, 3)
    except SnapshotError as e:
        stats.error("snapshot", str(e))
        save_run("failed")
        return SyncOutcome(run, 1)
    run["snapshot"] = snap.manifest()

    partial = False
    try:
        checkpoints = None if full else prior
        if mode == "initial":
            say("initial sync: no checkpoint, reading the whole source")
        elif full:
            say("full reconcile: re-reading the whole source and marking missing rows absent")
        else:
            say(f"incremental sync from checkpoint {_describe_checkpoint(prior)}")
        say("extracting")
        batch: list[Emit] = []
        seen_tables: set[str] = set()

        def flush() -> None:
            if not batch:
                return
            with cache.write():
                outcomes = cache.upsert_many([e.record for e in batch], observed_at=started, sync_run=run_urn)
                for k, v in outcomes.items():
                    written[k] = written.get(k, 0) + v
                obs = []
                for e in batch:
                    if e.observation:
                        o = e.observation
                        seen_tables.add(o.native_table)
                        obs.append(SourceObservation(e.record["urn"], adapter.source, scope, o.native_table, o.native_row_ids,
                                                     o.native_key, o.fingerprint, adapter.version, started, run_urn, o.minted))
                cache.observe_many(obs)
            batch.clear()

        with cache.bulk():
            for n, emit in enumerate(adapter.extract(snap, scope, cfg, stats, checkpoints, full=full), start=1):
                if not isinstance(emit, Emit):
                    raise IncompatibleSource('grouped extraction requires the incremental collector')
                batch.append(emit)
                if len(batch) >= BATCH:
                    flush()
                    say(f"processed {n} source records ({_fmt_written(written)})")
            flush()
        if stats.full_scan and mode == "incremental":
            run["mode"] = mode = "full_reconcile"
            say("source reset detected; this run became a full reconcile")
        say(f"extracted {_fmt_written(written)}")
        if full or mode in ("initial", "full_reconcile"):
            with cache.write():
                for t in seen_tables | stats.scanned_tables:
                    absent = cache.mark_absent(adapter.source, scope, t, started, run_urn)
                    if absent:
                        stats.bump(f"absent_from_source:{t}", absent)
        with cache.write():
            cache.set_checkpoint(adapter.source, scope, "adapter", stats.checkpoints)
        partial = any(e["code"] not in ("ambiguous_lid_phone_pair",) for e in stats.errors)
    except IncompatibleSource as e:
        stats.error("incompatible_schema", str(e))
        save_run("failed")
        return SyncOutcome(run, 4)
    except Exception as e:
        stats.error("extract_failed", f"{type(e).__name__}: {e}")
        save_run("failed")
        raise
    finally:
        snap.cleanup()

    save_run("partial" if partial else "complete")
    return SyncOutcome(run, 5 if partial else 0)


def _fmt_written(w: dict[str, int]) -> str:
    return f"new={w.get('inserted', 0)} updated={w.get('updated', 0)} unchanged={w.get('unchanged', 0)}"


def _describe_checkpoint(cp: dict[str, Any] | None) -> str:
    """Short, non-sensitive summary of an adapter checkpoint (scalar values only)."""
    if not cp:
        return "(empty)"
    parts = [f"{k}={v}" for k, v in sorted(cp.items()) if isinstance(v, (int, str, float))]
    return " ".join(parts) or "(opaque)"


def finalize(rec: dict[str, Any]) -> dict[str, Any]:
    return restamp(rec)
