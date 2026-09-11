# Testing

Synthetic fixtures drive all committed tests; nothing derived from a real database is ever
committed. Real-source runs are local, use read-only snapshots, and their outputs live under
`~/.cache/chatstore-dev/` or temp dirs.

## Verification contract

Before every commit:

```sh
.venv/bin/pytest -q                       # currently 70 tests, ~6 s
.venv/bin/ruff check src tests scripts
.venv/bin/mypy src
git status                                 # no *.sqlite / *.zip / *.jsonl tracked
git diff --cached | rg '(\+?44[0-9 ]{9,}|@s\.whatsapp\.net|@lid|@g\.us)'   # only 1555…/example.com fixtures allowed
```

CI (`.github/workflows/ci.yml`) runs the first three on macOS, including the Rust helper build.

## Suites

| File | Covers |
| --- | --- |
| `tests/unit/test_identity_vectors.py` | Published UUIDv5 vectors, tuple serialisation, Unicode/normalisation, alias stability |
| `tests/unit/test_schemas.py` | `specs/schemas/` stays in sync with `chatstore.canonical.schemas` |
| `tests/unit/test_cache.py` | Upsert/revision semantics, FTS keyed by rowid, bulk mode, heads |
| `tests/unit/test_helper_install.py` | Asset naming, tag resolution, sha256 verification fail-closed, atomic placement |
| `tests/integration/test_sync_cli.py` | Both adapters over synthetic DBs, incremental idempotence, CLI envelope, pagination, DST/date semantics, unsupported kinds |
| `tests/integration/test_archive.py` | Export/verify/import round trip, wrong password, byte flips, extra members, ZipCrypto, traversal/symlink/oversize members, revisions and lineage, branch conflicts, media hashes |
| `tests/integration/test_reconciliation.py` | Absence marking, source reset detection, incremental ≡ full, concurrency (readers during a writer), interrupted export |
| `tests/integration/test_curation.py` | People/links/suggestions, scope mapping aliases, conflict resolution, purge semantics |
| `tests/integration/test_faults.py` | Interrupted sync and import recovery, decompression bombs, forged declared sizes |
| `tests/integration/test_review_regressions.py` | One test per design-review finding: A→B→A revert, interrupted blob restore, `--no-media` then media import, stale digest rejection, emptied-bucket superseding revision, emptied-table reconciliation, exclusive `--until`, `availability` vs `local_state` |

Fixtures: `tests/fixtures/synthetic.py` builds a WhatsApp `ChatStorage.sqlite` (7 messages, one
with media) and a Messages `chat.db` with the real schemas but invented data (`1555…` numbers,
`example.com` addresses).

Practice adopted during the review: reproduce a finding as a failing test first, then change the
implementation.

## Final acceptance test

`scripts/acceptance.py [--media-month YYYY-MM]` runs against the real local sources (both apps
quit, ~25 min) in temporary data dirs and checks:

1. doctor runs
2. sync both sources
3. incremental re-sync is idempotent
4. export writes catalogue + buckets
5. re-export without changes writes nothing
6. verify all archives
7. import into clean dir
8. identical URNs and revision digests
9. revisions/observations/FTS counts equal
10. replies, reactions, edits, attachment metadata preserved
11. search suite equivalent
12. unresolved references reported honestly (= refs the source itself lacks)
13. repeated import produces no duplicates
14. included media hashes match
15. wrong password and tampered archive rejected, cache untouched
16. changed bucket revision imports without touching shared data
17. source databases unmodified by chatstore

Result: 17/17 at Phase 4 exit, after fixing two harness issues (sync_runs rows were counted as
non-idempotence; unresolved refs were compared against 0 instead of against the source's own
dangling reply/reaction targets, 14 on the test machine). After the design-review fixes a full
round trip was re-run manually: 81 archives, 662,299 identical `(urn, revision_digest)` pairs,
identical revision and FTS counts, 0 conflicts.

## Real-data validation conventions

- Read only from snapshots; never point tests at `~/Library` paths directly.
- Never print message bodies or contact details into a terminal transcript; report counts.
- Anything under `~/.cache/chatstore-dev/out` older than the current archive format rules is a
  stale artefact; re-export rather than trusting it.
