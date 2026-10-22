# Incremental preservation PR stack

Review the stack from bottom to top. Every PR targets the preceding branch, with
`codex/00-sync-reporting` targeting `master`. GitHub-native stack relationships are
managed through the official `github/gh-stack` extension (`gh stack`).

The bottom PR preserves the unpublished sync-reporting commit that was already on
local master. The remaining layers initially split the issue #10 implementation
without changing its source/test contents. Follow-up work stays in its owning layer,
including collector integration and archive byte limits. Cross-layer tests arrive
with the layer that provides their dependencies.

| Order | Branch | Responsibility | Cumulative tests at initial split |
| --- | --- | --- | ---: |
| 1 | `codex/00-sync-reporting` | Existing mode and write-outcome reporting | 79 |
| 2 | `codex/01-preservation-cache` | Transactional journal, freshness, scoped epochs and coverage | 113 |
| 3 | `codex/02-immutable-archives` | Immutable batches, exact receipts, import and remote durability proof | 134 |
| 4 | `codex/03-native-wal-monitor` | Pinned WAL views and live source/file-change routing | 178 |
| 5 | `codex/04-native-projection` | Page/row ownership, prepared mirror points, acknowledgement and rebase | 243 |
| 6 | `codex/05-durable-media` | Stable retained bytes and point-bound media observations | 258 |
| 7 | `codex/06-selective-adapters` | Native units, dependency routing and batched helper selection | 289 |
| 8 | `codex/07-collector-integration` | Canonical receipts/checkpoints, prepared sync and persistent polling | 419 |
| 9 | `codex/08-preservation-evidence` | Benchmark harnesses, raw counters and acceptance limits | 419 |

Validation for each cumulative layer uses the repository checks:

```sh
.venv/bin/ruff check src tests scripts
.venv/bin/mypy src
.venv/bin/pytest -q
```

The Rust helper was rebuilt from the repository source, including after the
selected-decoding changes. The original split preserved the pre-split source/test
tree; subsequent commits extend the implementation. Counts above record that initial
verification, rather than current test totals.

## Acceptance remains incomplete

These are draft PRs. They preserve the original scope of
[issue #10](https://github.com/lukeramsden/chatstore/issues/10) and do not close it.
Media watcher resource auditing, incremental auxiliary routing, bounded WAL
reset discovery and the complete end-to-end
workload/energy audit remain open. Header materialization and resource-limit
behavior also need the final acceptance audit.

[Preservation implementation and limits](incremental-preservation.md) records the
current guarantees and gaps. [Benchmark evidence](incremental-benchmark.md) separates
component measurements from end-to-end acceptance. Energy has not been measured.
