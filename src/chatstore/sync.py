"""Sync orchestration: diagnose → snapshot → extract → transactional import → FTS → report."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .adapters.base import (
    CapturedUnit,
    Emission,
    Emit,
    ExtractStats,
    IncompatibleSource,
    ReusedUnit,
    SourceAdapter,
)
from .adapters.snapshot import PermissionProblem, SnapshotError
from .cache import Cache, SourceObservation
from .canonical.records import now_ms, record, restamp
from .config import Config, Identity
from .paths import DataDir

if TYPE_CHECKING:
    from .cache.capture_coverage import CaptureCoverage
    from .prepared_sync import PreparedSync
    from .sync_session import SyncSession

BATCH = 5000


@dataclass
class SyncOutcome:
    run: dict[str, Any]
    exit_code: int  # 0 complete, 3 permission, 4 incompatible, 5 partial


def run_sync(adapter: SourceAdapter, dd: DataDir, cfg: Config, ident: Identity, cache: Cache, *,
             full: bool = False, session: SyncSession | None = None, prepared: PreparedSync | None = None, capture_state: CaptureCoverage | None = None, selected_message_rowids: set[int] | None = None, reuse_prepared_coverage: bool = False, progress: Callable[[str], None] | None = None) -> SyncOutcome:
    scope = ident.scope_for(adapter.source)
    if prepared is not None and session is not None:
        raise ValueError('prepared capture and live source monitoring cannot share a sync attempt')
    if reuse_prepared_coverage:
        if prepared is None or capture_state is None or full or selected_message_rowids != set():
            raise ValueError('prepared coverage reuse requires complete retained selective coverage')
        with cache.write():
            if capture_state.full:
                raise ValueError('prepared coverage reuse cannot skip full reconciliation')
    run_urn = prepared.run_urn if prepared is not None else uuid.uuid4().urn
    started = prepared.observed_at_ms if prepared is not None else now_ms()
    stats = ExtractStats(selected_message_rowids=selected_message_rowids)
    written: dict[str, int] = {"inserted": 0, "updated": 0, "unchanged": 0}
    coverage_rebuild = capture_state is not None and selected_message_rowids is None
    mode = "initial"
    prior = cache.get_checkpoint(adapter.source, scope, "adapter")
    if prior is not None:
        mode = "full_reconcile" if full or coverage_rebuild else "incremental"
    run: dict[str, Any] = {
        "started_at": started, "finished_at": None, "status": "running", "mode": mode, "snapshot": None,
        "checkpoints": None, "counts": {}, "coverage": None, "errors": [], "adapter_version": adapter.version,
        "parser_version": None,
    }

    def save_run(status: str) -> None:
        run["status"] = status
        run["finished_at"] = None if status == "running" else now_ms()
        # `written` is measured at the cache and means the same for every adapter: how many emitted
        # records were new, changed, or identical to what was already stored. The other keys are the
        # adapter's own tallies of what it emitted (which may be a full re-read on incremental runs).
        run["counts"] = dict(stats.counts) | {"unsupported": dict(stats.unsupported), "written": dict(written)}
        run["errors"] = list(stats.errors)
        coverage = session.coverage(cache, adapter.source, scope) if session is not None else None
        run["coverage"] = (coverage if coverage is not None else cache.coverage(adapter.source, scope)) | {"notes": list(stats.notes)}
        run["checkpoints"] = dict(stats.checkpoints) or None
        rec = record("sync_runs", run_urn, adapter.source, scope, **run)
        with cache.write():
            if status == 'complete' and prepared is not None:
                prepared.validate(adapter, cfg, scope)
                prepared.check_parent(cache, adapter.source, scope)
            if status == 'complete' and capture_state is not None:
                state = capture_state.finish(at=started, run=run_urn)
                if state['absent']:
                    stats.bump('absent_from_source:withdrawn', state['absent'])
                    run['counts'] = dict(stats.counts) | {'unsupported': dict(stats.unsupported), 'written': dict(written)}
                    rec = record('sync_runs', run_urn, adapter.source, scope, **run)
            cache.upsert(rec, observed_at=run['finished_at'] or started, sync_run=run_urn)
            if status == 'complete':
                cache.set_checkpoint(adapter.source, scope, 'adapter', stats.checkpoints)
                if prepared is not None:
                    prepared.commit_receipt(cache, adapter.source, scope, rec)

    save_run("running")
    say = progress or (lambda s: None)
    if prepared is None:
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

    if session is not None:
        session.prepare(adapter, cfg, cache, scope)
        if not full:
            count = session.reuse(cache, stats, started, run_urn)
            if count is not None:
                written['unchanged'] = count
                say('unchanged source: reusing exact monitored coverage')
                save_run('complete')
                return SyncOutcome(run, 0)
        session.begin(cache, stats)

    if prepared is not None:
        snap = prepared.snapshot(include_auxiliary=not reuse_prepared_coverage)
    else:
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

    from .cache.extraction import ExtractionMemo, code_signature
    partial = False
    try:
        memo = ExtractionMemo(cache, adapter.source, scope, code_signature(adapter, cfg), reuse=not full)
        stats.memo = memo
        checkpoints = None if full or coverage_rebuild else prior
        if reuse_prepared_coverage:
            stats.checkpoints.update(prior or {})
            stats.counts['prepared_coverage_reused'] = 1
            stats.notes.append('unchanged prepared source; exact retained coverage re-sighted without adapter extraction')
        if mode == "initial":
            say("initial sync: no checkpoint, reading the whole source")
        elif full:
            say("full reconcile: re-reading the whole source and marking missing rows absent")
        else:
            say(f"incremental sync from checkpoint {_describe_checkpoint(prior)}")
        say("extracting")
        batch: list[Emission] = []
        batch_weight = 0
        processed = 0
        seen_tables: set[str] = set()

        def capture(records: list[Emit]) -> None:
            outcomes = cache.upsert_many([e.record for e in records], observed_at=started, sync_run=run_urn)
            for k, v in outcomes.items():
                written[k] = written.get(k, 0) + v
            obs = []
            for e in records:
                if e.observation:
                    o = e.observation
                    seen_tables.add(o.native_table)
                    obs.append(SourceObservation(e.record['urn'], adapter.source, scope, o.native_table, o.native_row_ids,
                                                 o.native_key, o.fingerprint, adapter.version, started, run_urn, o.minted))
            cache.observe_many(obs)
            if capture_state is not None:
                capture_state.stage_records(records)

        def flush() -> None:
            if not batch:
                return
            with cache.write(), capture_state.writing() if capture_state else session.writing(cache) if session else nullcontext():
                records: list[Emit] = []
                captured: list[CapturedUnit] = []
                reused: list[ReusedUnit] = []

                def commit_records() -> None:
                    if records or captured:
                        capture(records)
                        memo.save(captured)
                        records.clear()
                        captured.clear()

                for item in batch:
                    if isinstance(item, ReusedUnit):
                        commit_records()
                        item = memo.revalidate(item, stats)
                    if isinstance(item, ReusedUnit):
                        reused.append(item)
                        written['unchanged'] += item.record_count
                    elif isinstance(item, CapturedUnit):
                        records.extend(item.records)
                        captured.append(item)
                    else:
                        records.append(item)
                commit_records()
                memo.sight(reused, started, run_urn)
                if capture_state is not None:
                    capture_state.stage([span for item in reused for span in item.record_ranges],
                                        [span for item in reused for span in item.observation_ranges])
            batch.clear()

        with cache.bulk():
            emissions = () if reuse_prepared_coverage else adapter.extract(snap, scope, cfg, stats, checkpoints, full=full)
            for item in emissions:
                count = len(item.records) if isinstance(item, CapturedUnit) else item.record_count if isinstance(item, ReusedUnit) else 1
                weight = max(1, count)
                if weight > BATCH and isinstance(item, CapturedUnit):
                    flush()
                    batch_weight = 0
                    # A very large message is captured in bounded transactions, without memo reuse.
                    for offset in range(0, len(item.records), BATCH):
                        with cache.write(), capture_state.writing() if capture_state else session.writing(cache) if session else nullcontext():
                            capture(item.records[offset:offset + BATCH])
                    with cache.write():
                        cache.conn.execute('UPDATE native_units SET valid=0 WHERE source=? AND scope=? AND unit_key=?',
                                           (adapter.source, scope, item.key))
                    processed += count
                    continue
                if batch_weight + weight > BATCH:
                    flush()
                    batch_weight = 0
                    say(f'processed {processed} source records ({_fmt_written(written)})')
                batch.append(item)
                batch_weight += weight
                processed += count
                if batch_weight >= BATCH:
                    flush()
                    batch_weight = 0
                    say(f'processed {processed} source records ({_fmt_written(written)})')
            flush()
        if stats.full_scan and mode == "incremental":
            run["mode"] = mode = "full_reconcile"
            say("source reset detected; this run became a full reconcile")
        say(f"extracted {_fmt_written(written)}")
        if stats.counts.get('canonical_units_reused') or stats.counts.get('canonical_units_rebuilt'):
            say(f"canonical groups rebuilt={stats.counts.get('canonical_units_rebuilt', 0)} reused={stats.counts.get('canonical_units_reused', 0)}; native inputs checked for every group")
        complete_tables = set(stats.scanned_tables)
        if stats.selected_message_rowids is None and (full or mode in ('initial', 'full_reconcile')):
            complete_tables |= seen_tables
        if complete_tables and capture_state is None:
            with cache.write(), capture_state.writing() if capture_state else session.writing(cache) if session else nullcontext():
                for t in complete_tables:
                    absent = cache.mark_absent(adapter.source, scope, t, started, run_urn)
                    if absent:
                        stats.bump(f"absent_from_source:{t}", absent)
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
    if session is not None and not partial:
        session.finish(cache, stats, run_urn, run['coverage'])
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
