# History

How chatstore was delivered. The original implementation plan was written up front with four
phases, an acceptance test and a list of decisions to confirm; this page records what each phase
produced and where the plan was corrected by evidence. The plan's substance now lives in
[overview.md](overview.md), [architecture.md](architecture.md), [sources.md](sources.md),
[decisions.md](decisions.md) and the [specs](../specs/README.md); the plan document itself is
retired.

## Phase 1 — contracts and identity (`fad84fd`, 2026-09-10)

- Package layout, `pyproject.toml`, MIT.
- `specs/canonical-model.md`, `specs/identity.md`, `specs/archive-format.md`, `specs/cli-json.md`;
  JSON Schemas generated from code; published UUIDv5 test vectors; namespace frozen.
- Native-key validation against both real sources (`specs/source-findings.md`): WhatsApp stanza-ID
  uniqueness within the identity tuple, duplicate history-sync rows, Messages GUID uniqueness,
  orphan-row prevalence, timestamp units, reaction/reply/edit counts.
- Pins chosen: `imessage-database =4.2.0`, Rust 1.95, `pyzipper 0.4.0` after interop and
  fail-closed evaluation.
- Exit criterion met: identifiers and canonical fixtures round-trip without source apps.

## Phase 2 — ingestion, cache, CLI, skill (`98de4c3`)

- Read-only snapshotting via the SQLite backup API; `doctor` diagnostics incl. Full Disk Access.
- WhatsApp adapter (duplicate collapse, LID aliases, media availability) and Messages adapter with
  the Rust `chatstore-messages-decoder` helper (framed versioned JSONL).
- SQLite/FTS5 cache with revisions and observations; `sync` (initial/incremental);
  `search`, `chats`, `people`, `read`, `resolve`, `context`, `rebuild-index`; JSON envelope; exit
  codes; `skills/chatstore/SKILL.md`.
- Full ingestion of the test machine validated: 195,783 + 77,198 messages, 0 spurious revisions on
  re-sync.

## Phase 3 — encrypted archives and restore (`a8dbf79`)

- Monthly UTC buckets + catalogue, WinZip-AES-256 single-`payload` container, manifest with
  lineage and `payload_digest` over checksums, immutable revisions, atomic publish.
- Fail-closed verify/import: wrong password, tampering, extra members, ZipCrypto, unsafe TAR
  members, size/ratio limits; idempotent import; FTS rebuild; scope adoption.
- Correction: attachment records exported verbatim (rewriting `availability` broke digests).
- Measured: 81 text archives in 4m51s; restore 3m52s with an identical current view.

## Phase 4 — reconciliation, curation, hardening (`d366d5d`, `ab6ca63`)

- Full reconciliation and source-reset detection (`absent_from_source`), identity links and
  suggestions, scope mapping via aliases, conflict inspection/resolution, purge.
- `docs/security.md`, `docs/integration.md`; `scripts/acceptance.py` — 17/17 on real sources
  after two harness fixes.
- Added reconciliation/interruption/DST/concurrency tests.

## Post-acceptance features (2026-09-11)

- Sync/import performance: batched upserts and deferred WAL checkpoints (282 s → 119 s);
  fault-injection tests (`73dddb9`).
- `media status|list` (`62a39ad`).
- `archive export --context minimal`; bucket digests exclude context (`103f855`).

## Design review and corrections (`1b2d08a`, `2123e3a`)

A read-only review against Ousterhout's *A Philosophy of Software Design* checklist found seven
behavioural issues and several structural ones; each behavioural finding was reproduced as a
failing test in `tests/integration/test_review_regressions.py` before being fixed:

1. A → B → A edit left B current after restore (revision identity).
2. Interrupted or `--no-media` import could never restore media later.
3. Archive validation trusted producer digests and skipped several record kinds and counts.
4. An emptied bucket produced no superseding archive.
5. Full sync never reconciled a completely emptied table.
6. `--until YYYY-MM-DD` was inclusive despite the spec.
7. `media status` reported source availability as local presence.

Structural: typed `cli/common.py`, `SourceObservation`, cache-owned record transitions, shared
export tail, `media.py`. Details in [decisions.md](decisions.md).

## Publication (`334e4b9`, `c056925`, v0.1.0)

- Public GitHub repository, MIT `LICENSE`, CI on macOS.
- Release workflow building the helper for arm64 and x86_64 with `SHA256SUMS`;
  `chatstore helper install|status`; `doctor` points at it. Version 0.1.0 tagged.

## Plan corrections worth remembering

| Plan assumption | What evidence showed |
| --- | --- |
| Messages incremental sync could use a row-id checkpoint | Edits, receipts and tapbacks mutate old rows; full re-decode + fingerprints is cheap enough |
| Attachment `availability` could be rewritten to `not_exported` in text archives | Breaks the content digest; count it in the manifest instead |
| Import could order revisions by ancestry chain | Digests identify content; re-observed content must update `observed_at`; heads are by observation time within lineage |
| Container authentication was sufficient validation | Producer bugs yield authentic but invalid records; recompute every digest |
| Reconciling tables that emitted rows was enough | Emptied tables emit nothing; adapters must report what they scanned |
| Prebuilt helper binaries were "future work" | Shipped in v0.1.0 via the release workflow |
