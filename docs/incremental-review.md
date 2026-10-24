# Incremental preservation review

The implementation is consolidated into one PR against `master`. The earlier nine draft
PRs are superseded. Review these areas in dependency order:

1. `cache/changefeed.py`, freshness and coverage: transactional journal and independent checkpoints.
2. `archive/incremental.py`, verification and delivery: immutable authenticated batches, exact ciphertext receipts and cold restore.
3. Source monitoring, pinned WAL views, page ownership and native mirror: stable captures and recovery.
4. Adapter dependency routing and retained media: historical changes and point-bound bytes.
5. Prepared sync and persistent sessions: canonical publication, acknowledgement and retry.

The full suite passes 557 tests. Ruff and mypy are the repository static checks. See
[incremental-preservation.md](incremental-preservation.md) for implementation details and
[incremental-benchmark.md](incremental-benchmark.md) for measurements and their limits.

## Issue #10 acceptance checklist

- Implemented: transactional historical changes and revisions; immutable baseline/deltas;
  exact verified receipts; independent consumer checkpoints; retries and archive-only restore.
- Implemented with synthetic evidence: lightweight clean polls and selective message changes
  during valid monitored sessions; identity/context and media-availability preservation.
- Implemented: soft bulk WAL checkpoint budget, with committed-batch, rollback and pinned-reader tests.
- Source bound: vnode monitoring has a measured descriptor ceiling. Above it, coverage is
  rejected or reconciled rather than silently reused; this is not unbounded file-monitor support.
- Implemented: selective group-event changes, including duplicate-owner coverage, rollback and offline retry.
- Implemented: selective chat archive/removal flags and group titles, with full-extraction parity and duplicate-owner coverage.
- Conservative metadata path: auxiliary/identity/structural-chat and direct-name changes can
  rebuild broader headers. Message extraction remains selected; the named identity/context
  acceptance case verifies preservation and small delta output, not selective header emission.
- Accepted source limitation: WAL loss/reset may require full-source reconciliation to preserve
  historical edits and deletions. The user explicitly accepted this fallback on 22 October 2026.
  Source databases remain read-only; recovery work scales with source size. Compatible fresh-WAL
  recovery avoids canonical rebuilding but still compares the source image.
- Measured: 100,000-message text-only capture/encrypted export/verified import pipelines for both adapters.
- Measured: populated 500,000-message WhatsApp pipeline with 475,000 media rows, 1,000 chats
  and 5,000 watched files; all 57 timed stages and final assertions passed. See
  [raw results](incremental-whatsapp-population.json). Initial capture/import remain expensive;
  first post-baseline clean capture and later clean captures are reported separately.
- Measured locally: representative populated capture, encrypted export and verified import,
  with wall/CPU, attributed storage and sampled memory for every stage.
- Measured remotely: a separate 100,000-message canonical history transferred over real SSH
  and imported on the supplied Linux host, with interrupted upload, rejected partial archive,
  retry and archive-only historical/context/provenance assertions. See [raw results](incremental-remote-pipeline.json).
  SSH sent/received counters measure transport bytes; the populated 500,000-row fixture
  still reports ciphertext volume rather than a measured transfer of that same fixture.
- Unavailable: direct energy instrumentation. CPU/I/O counters do not establish energy use.

The concrete design and acceptance cases below have local, Linux and real-transport evidence.
Source support bounds remain explicit under requirement 8: this does not claim that every
metadata fallback is strictly proportional to changed rows, or that every file population fits
the OS descriptor limits. WAL-loss reconciliation was explicitly accepted by the user. No
retention or capture cadence reduction is used to obtain the reported improvements.

## Evidence against the requested design

The numbered rows below follow issue #10. Test names identify preservation evidence,
not proof of the full performance requirement.

| Requirement | Current evidence | Remaining qualification |
| --- | --- | --- |
| 1. Transactional changes, including old content and non-text changes | `test_next_poll_captures_old_source_edits_absence_context_and_media`, `test_context_and_media_only_mutations`, `test_identity_only_change_and_forwarded_freshness_survive` | Source monitoring must retain complete coverage; fallback reads remain documented. |
| 2. Skip unchanged history before materialization | `test_baseline_delta_expiry_cold_restore_and_receipt` traces clean export SQL; `test_whatsapp_canonical_coverage_handles_clean_old_edit_backfill_and_delete` checks selected extraction | Restart, lost WAL and unsupported sources can require reconciliation. |
| 3. Immutable baseline and deltas, retained revisions | `test_baseline_delta_expiry_cold_restore_and_receipt` checks lineage, superseded revisions and tombstones | No ciphertext or captured-content pruning is introduced. |
| 4. Exact verified-export evidence and reduced duplicate passes | `test_retry_hashes_exact_ciphertext_without_redecrypting`; receipts bind ciphertext SHA-256, export ID and feed position | Caller must consume the receipt API instead of invoking a redundant verification command. |
| 5. Incremental import and remote durability proof | `test_clean_sighting_import_uses_intervals_without_reading_entity_mappings`, `test_remote_durability_requires_metadata_and_remote_bytes`, `test_https_delivery_retrieves_both_objects_and_retries_truncated_transfer` | Local HTTPS tests exercise remote retrieval proof and truncated response retry; the separate 100,000-message real-SSH/Linux run measures transfer and import. |
| 6. Crash safety and independent checkpoints | `test_sync_rollback_and_export_crash_recovery`, `test_interrupted_import_is_atomic`, `test_independent_consumers_bounded_batches_and_migration`, `test_overlapping_consumers_do_not_replay_or_conflict` | Tests inject failures at named boundaries; they do not simulate every OS/power-loss timing. |
| 7. Archive-only cold restore | `test_baseline_delta_expiry_cold_restore_and_receipt`, `test_shared_file_changes_missing_and_downloads_cold_restore_all_versions`, `test_prepared_aliases_survive_source_expiry_and_cold_restore` | Byte-including archives are required to restore attachment bytes; text-only policy remains explicit. |
| 8. Document source limitations without weakening preservation | `docs/incremental-preservation.md`, `docs/sources.md`; monitor failure and empty-WAL fallback tests | Descriptor limits and conservative reconciliation paths are explicit support bounds; unproven coverage is never silently treated as clean. |

The populated 500,000-row local pipeline and separate 100,000-message remote pipeline are
different measured workloads. Preservation tests cover failure and source-expiry boundaries;
benchmark results establish the reported costs, not universal latency or energy guarantees.

## Platform checks

The macOS CI job builds the source decoder and runs all checks. A separate Linux
job runs archive round trips, incremental preservation/recovery, archive validation
and real HTTPS delivery tests without source apps or the macOS decoder. It exercises
the importer/delivery platform directly; source collection remains macOS-specific.
