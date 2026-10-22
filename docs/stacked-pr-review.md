# Incremental preservation review

The implementation is consolidated into one PR against `master`. The earlier nine draft
PRs are superseded. Review these areas in dependency order:

1. `cache/changefeed.py`, freshness and coverage: transactional journal and independent checkpoints.
2. `archive/incremental.py`, verification and delivery: immutable authenticated batches, exact ciphertext receipts and cold restore.
3. Source monitoring, pinned WAL views, page ownership and native mirror: stable captures and recovery.
4. Adapter dependency routing and retained media: historical changes and point-bound bytes.
5. Prepared sync and persistent sessions: canonical publication, acknowledgement and retry.

The full suite passes 534 tests. Ruff and mypy are the repository static checks. See
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
