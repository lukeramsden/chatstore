# Incremental preservation review

The implementation is consolidated into one PR against `master`. The earlier nine draft
PRs are superseded. Review these areas in dependency order:

1. `cache/changefeed.py`, freshness and coverage: transactional journal and independent checkpoints.
2. `archive/incremental.py`, verification and delivery: immutable authenticated batches, exact ciphertext receipts and cold restore.
3. Source monitoring, pinned WAL views, page ownership and native mirror: stable captures and recovery.
4. Adapter dependency routing and retained media: historical changes and point-bound bytes.
5. Prepared sync and persistent sessions: canonical publication, acknowledgement and retry.

The full suite passes 535 tests. Ruff and mypy are the repository static checks. See
[incremental-preservation.md](incremental-preservation.md) for implementation details and
[incremental-benchmark.md](incremental-benchmark.md) for measurements and their limits.

## Issue #10 acceptance checklist

- Implemented: transactional historical changes and revisions; immutable baseline/deltas;
  exact verified receipts; independent consumer checkpoints; retries and archive-only restore.
- Implemented with synthetic evidence: lightweight clean polls and selective message changes
  during valid monitored sessions; identity/context and media-availability preservation.
- Open: scalable file monitoring above the measured vnode descriptor ceiling.
- Open: fully incremental auxiliary/header changes and bounded source recovery after WAL loss.
  Compatible fresh-WAL recovery avoids canonical rebuilding but still compares the source image.
- Open: complete representative end-to-end acceptance measurements, including remote transfer
  and energy where available. Existing CPU/I/O/ciphertext counters do not measure energy.

The PR must not claim to close #10 until these gaps are resolved or the issue's acceptance
criteria explicitly accept the documented source limitations. No retention or capture cadence
reduction is used to obtain the reported improvements.

## Evidence against the requested design

The numbered rows below follow issue #10. Test names identify preservation evidence,
not proof of the full performance requirement.

| Requirement | Current evidence | Remaining qualification |
| --- | --- | --- |
| 1. Transactional changes, including old content and non-text changes | `test_next_poll_captures_old_source_edits_absence_context_and_media`, `test_context_and_media_only_mutations`, `test_identity_only_change_and_forwarded_freshness_survive` | Source monitoring must retain complete coverage; fallback reads remain documented. |
| 2. Skip unchanged history before materialization | `test_baseline_delta_expiry_cold_restore_and_receipt` traces clean export SQL; `test_whatsapp_canonical_coverage_handles_clean_old_edit_backfill_and_delete` checks selected extraction | Restart, lost WAL and unsupported sources can require reconciliation. |
| 3. Immutable baseline and deltas, retained revisions | `test_baseline_delta_expiry_cold_restore_and_receipt` checks lineage, superseded revisions and tombstones | No ciphertext or captured-content pruning is introduced. |
| 4. Exact verified-export evidence and reduced duplicate passes | `test_retry_hashes_exact_ciphertext_without_redecrypting`; receipts bind ciphertext SHA-256, export ID and feed position | Caller must consume the receipt API instead of invoking a redundant verification command. |
| 5. Incremental import and remote durability proof | `test_clean_sighting_import_uses_intervals_without_reading_entity_mappings`, `test_remote_durability_requires_metadata_and_remote_bytes`, `test_https_delivery_retrieves_both_objects_and_retries_truncated_transfer` | A separate local HTTPS test exercises the real retrieval path and a truncated response retry; no remote-host throughput measurement is available. |
| 6. Crash safety and independent checkpoints | `test_sync_rollback_and_export_crash_recovery`, `test_interrupted_import_is_atomic`, `test_independent_consumers_bounded_batches_and_migration`, `test_overlapping_consumers_do_not_replay_or_conflict` | Tests inject failures at named boundaries; they do not simulate every OS/power-loss timing. |
| 7. Archive-only cold restore | `test_baseline_delta_expiry_cold_restore_and_receipt`, `test_shared_file_changes_missing_and_downloads_cold_restore_all_versions`, `test_prepared_aliases_survive_source_expiry_and_cold_restore` | Byte-including archives are required to restore attachment bytes; text-only policy remains explicit. |
| 8. Document source limitations without weakening preservation | `docs/incremental-preservation.md`, `docs/sources.md`; monitor failure and empty-WAL fallback tests | Per-file descriptor scaling and full-reconciliation cases remain acceptance limits. |

Measured synthetic performance and preservation tests are separate evidence. Neither
substitutes for the requested representative full-workload audit.
