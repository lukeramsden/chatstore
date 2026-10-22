# Synthetic preservation benchmark — 18 October 2026

Measured on `macOS-26.6.2-arm64-arm-64bit-Mach-O`, Python `3.14.3`. The fixture contains 100,000 extra synthetic WhatsApp messages plus the standard small fixture, spread across roughly six years. Each stage runs in a fresh process; these are single-run measurements, not controlled energy measurements. The large run preceded final producer-event deduplication and validation cleanup; the entire harness also passed with 1,000 messages on the final implementation.

**Issue #10 remains partially implemented.** Archive work is incremental; source sync still scans history. The initial baseline and cold restore are intentionally proportional to retained history.

| Stage | Wall s | CPU s | Peak RSS MiB | Storage read MiB | Storage written MiB | Archive output KiB |
|---|---:|---:|---:|---:|---:|---:|
| sync-initial | 31.8977 | 26.5816 | 129.8 | 1286.844 | 4669.574 | 0.000 |
| monthly-baseline | 76.4489 | 68.8776 | 1842.9 | 1148.566 | 922.586 | 45913.534 |
| export-baseline | 72.5679 | 68.5037 | 202.4 | 870.695 | 991.621 | 28855.430 |
| sync-poll | 21.5439 | 13.2137 | 138.8 | 1732.496 | 1242.676 | 0.000 |
| monthly-poll | 21.6518 | 6.3582 | 1685.0 | 1076.336 | 0.145 | 6.256 |
| export-poll | 0.0337 | 0.0328 | 37.0 | 0.090 | 0.078 | 2.305 |
| export-unchanged | 0.0004 | 0.0004 | 32.7 | 0.000 | 0.000 | 0.000 |
| sync-new-message | 19.5647 | 12.7157 | 137.8 | 1465.254 | 1102.594 | 0.000 |
| monthly-new-message | 20.9619 | 6.6604 | 1749.3 | 1169.621 | 3.422 | 173.401 |
| export-new-message | 0.0327 | 0.0318 | 37.5 | 0.078 | 0.102 | 3.170 |
| sync-old-edit | 17.4823 | 12.0272 | 135.9 | 455.480 | 1471.668 | 0.000 |
| export-old-edit | 0.0327 | 0.0325 | 36.8 | 0.000 | 0.109 | 3.311 |
| verify-new-delta | 0.0301 | 0.0293 | 36.5 | 0.008 | 0.035 | 0.000 |
| import-baseline | 117.6320 | 87.0051 | 174.4 | 1884.969 | 13875.902 | 0.000 |
| import-poll | 0.3749 | 0.1124 | 52.9 | 34.082 | 0.414 | 0.000 |
| import-new-message | 0.0957 | 0.0933 | 53.6 | 0.152 | 1.246 | 0.000 |
| import-old-edit | 0.0969 | 0.0941 | 53.7 | 0.211 | 1.070 | 0.000 |
| import-cold-all | 121.9201 | 87.8818 | 178.2 | 2185.082 | 13812.031 | 0.000 |
| sync-initial-before-cache-cap | 32.5100 | 26.6155 | 689.0 | 1618.051 | 4109.797 | 0.000 |
| export-cli-unchanged | 0.0207 | 0.0197 | 36.2 | 0.129 | 0.004 | 0.000 |

After a no-change source poll, changefeed export used 0.034 s and 37 MiB instead of the monthly path’s 21.65 s and 1,685 MiB. It sent 2.36 KiB of coverage metadata. With no journal changes it emitted no archive. The full JSON CLI path, including freshness reporting, took 0.021 s after migration.

One new message produced 3,246 bytes rather than the monthly replacement’s 177,563 bytes; export wall time fell from 20.96 s to 0.033 s. A historical edit produced 3,390 bytes. Verifying that object took 0.030 s; importing it took 0.097 s. Initial delta baseline output was about 29.55 MB compared with 47.02 MB for monthly archives.

Source polling remained 17–22 s and wrote more than 1 GB in these runs. That is the principal remaining end-to-end cost. The final 64 MiB SQLite cache cap reduced initial-sync peak RSS from 689 MiB to 130 MiB with similar wall/CPU time; the final fixture also includes the benchmark new message and old edit.

The delta baseline imported in 117.6 s; an isolated restore of all delta objects took 121.9 s with no source database access. Freshness-only import took 0.375 s, including producer-to-local ID mapping, without rewriting all observation bodies.

A separate trial putting all 100,000 extra messages in a single month hit the legacy 64 MiB per-member verification limit (`records/messages.jsonl` was 117,476,327 bytes). No archive/checkpoint was accepted for that oversized bucket. The spread fixture above avoids that limit, and bounded changefeed batches do not require a whole-month member.

Kernel storage counters include cache misses, WAL/checkpoint activity and writeback; they are not logical SQLite byte counts. Memory includes process imports and initial cache setup; wall/CPU stage timing excludes module imports/configuration/Cache construction. Archive output is a transfer-size proxy: no network upload, remote server benchmark or energy-joule measurement was performed. macOS wakeup counters are available in the raw results and are not converted to energy.

## Compact local sightings

A subsequent source-sync optimization stores unchanged head and observation sightings as disjoint,
coalescing ID intervals. It avoids rewriting canonical JSON and provenance rows each poll, while
keeping each exact sighting range/time/run in the durable journal. These measurements use the
same 100,000-message workload; source snapshots and native row reconstruction still occur.

| No-change sync | Wall s | CPU s | Peak RSS MiB | Kernel read MB | Kernel written MB |
|---|---:|---:|---:|---:|---:|
| Before compact local sightings | 21.544 | 13.214 | 138.8 | 1816.7 | 1303.0 |
| Compact sightings and covering head lookup | 13.650 | 9.810 | 128.3 | 620.7 | 16.54 |

The write reduction is about 98.7%. Wall/read figures vary with cache state and host activity;
these are single-run measurements. The source still performs work proportional to its native
history, so issue #10 is not complete. Initial capture gained stable entity IDs and a covering
head index; the first compaction run's initial sync took 61.9 s and wrote about 6.03 GB, an
upfront cost that is not hidden by the poll comparison.

An intermediate cProfile run attributed 12.5 of 21.5 seconds to adapter extraction/reconstruction,
including repeated canonical JSON/digest and URN generation. Native extraction reuse, described below, now removes repeated canonical reconstruction
while continuing to check historical native inputs. Receiver range-ID translation originally iterated mapped IDs; the interval projection
described below replaces that path. The full 1,000-message
sync/export/verify/import/cold-restore harness also passed with the compact sighting format.

[Source-sync counters](incremental-sync-benchmark.json) · [Intermediate profile](incremental-sync-profile.txt)

[Raw results](incremental-benchmark.json) · [Reproduction and limitations](incremental-preservation.md#measurements)

## Native extraction reuse

A warm no-change WhatsApp poll reused all 100,008 logical message groups, rebuilt none,
and wrote no canonical changes (200,037 unchanged records). Wall time was **6.715 s**,
CPU **5.109 s**, parent peak RSS **127.1 MiB**, kernel reads **324.0 MB** and writes
**16.41 MB**. This single run follows a policy-signature warmup; cache state affects reads
and time. The raw result predates moving cache close into the harness's timed stage.

A preceding fresh 100,000-extra-message run measured initial capture at 98.085 s / 55.676 CPU s,
with 6.525 GB of kernel writes. Its no-change, new-message and old-edit polls took 8.009,
8.396 and 6.623 s respectively. These are different runs, not a controlled energy benchmark.
The extra initial memo/index cost is an upfront cost, not hidden in the warm comparison.

Coverage tests exercise old text/status changes, data-only and parent/context dependencies,
identity references, media replacement/disappearance, native deletion, local purge/edits,
policy changes, shared entities/evidence, transactional rollback and writer revalidation.
The full suite currently passes 220 tests. Source snapshots/scans and Messages helper decoding
still grow with history; receiver sighting-range translation now uses intervals as described below. Issue #10 remains open.

[Warm WhatsApp extraction counters](incremental-extraction-benchmark.json)

The final 1,000-extra-message harness passed independently for both WhatsApp and Messages,
including baseline/delta export, verification, replay and cold restore with source/cache
directories moved offline. A first large Messages run was stopped after identifying a
missing synthetic `message(thread_originator_guid)` index: the library's correlated reply
count otherwise scans the full message table for every row. The harness now creates that
index alongside join indexes. This changes only synthetic fixture preparation.

The indexed Messages fixture completed all four source stages:

| Stage | Wall s | CPU s, parent + helper | Groups rebuilt | Groups reused |
|---|---:|---:|---:|---:|
| Initial capture | 51.193 | 38.536 | 100,007 | 0 |
| No change | 6.143 | 4.330 | 0 | 100,007 |
| New message | 4.248 | 4.396 | 1 | 100,007 |
| Historical edit | 5.536 | 4.792 | 1 | 100,007 |

The no-change poll reported 142.6 MiB parent peak RSS, 45.6 MiB helper peak RSS and
28.57 MB parent storage writes. Process peaks are separate lifetime maxima, not a measured
simultaneous total. Initial capture reported 6.405 GB parent storage writes. Child CPU is
included above; child storage bytes and energy are not measured. The helper still streams
every message, so canonical reuse does not establish bounded native work. Both native and
cache data in this benchmark are synthetic.

## Imported sighting translation

A synthetic in-memory 100,000-key microbenchmark compared the original per-key query/materialize/
sort path with the interval projection. Clean translation took 0.067 ms versus 165.5 ms; one
locally divergent head took 0.277 ms versus 126.3 ms. Building the initial projection took
130.3 ms. These single-run figures isolate translation, exclude archive I/O and capture, and
are not end-to-end import or energy measurements. Fragmented mappings require more intervals.

The full 1,000-extra-message WhatsApp sync/export/verify/import/cold-restore harness passed
with this projection. Tests also cover fragmented/reordered mappings, missing keys, rollback,
head divergence/restoration, multiple producers and provenance divergence/purge through the
actual importer. The no-change import test denies canonical and per-key mapping reads.

Reproduce with `.venv/bin/python scripts/benchmark_import_ranges.py --keys 100000`.
[Raw translation results](incremental-import-ranges-benchmark.json).

## Stable monitored source polls

With a retained `SyncSession`, two captures establish complete media watch coverage. Subsequent
stable polls reuse exact sightings without snapshots, canonical reconstruction, full message-index
counts or Messages helper invocation. Warm synthetic 100,000-extra-message histories measured:

| Source, fourth retained poll | Wall s | CPU s | Parent kernel read MB | Parent kernel written MB |
|---|---:|---:|---:|---:|
| WhatsApp | 0.01427 | 0.00967 | 0.344 | 0.705 |
| Messages | 0.01199 | 0.00995 | 0.115 | 0.672 |

Each source reported `monitored_coverage_reused=1`, no canonical changes, and exact sighting
ranges for roughly 200,000 records and their observations. These are single-process warm runs,
not cold starts or energy measurements. The retained process's lifetime RSS peak was about
174 MiB, including earlier capture/cache migration; it does not measure fast-poll-only peak RSS.
No helper child ran during the stable polls. Storage counters are parent-only; no bytes were
uploaded and no energy joules were measured.

The same sessions' first two polls still reconciled native history: WhatsApp took 11.42 and
7.73 s, Messages 12.11 and 8.21 s. They used existing canonical caches, not fresh source ingestion.
Cache construction/index migration is excluded from stage timing and may require a one-time
history read. Summaries now combine current capture rows and sighting intervals through indexes;
an intermediate implementation's extra 1.7 GB canonical scan was removed before these results.
The raw counters include both warmup and stable polls; no warmup cost is hidden.

Reproduce against fresh synthetic history with
`.venv/bin/python scripts/benchmark_incremental.py --messages 100000 --monitored-polls 4`
(and `--source messages` for Messages). Fresh ingestion and index construction costs will differ
from the existing-cache warm fixture above. CLI users retain the monitor with
`chatstore --json sync --watch-interval 900`; one-shot sync does not retain it across processes.

Changes, lost monitors, unsupported platforms, symlinks and watch resource limits still require
reconciliation. Cache generations are now isolated by source and account scope; unrelated source capture
does not force a scan. This isolation is covered by cross-adapter integration tests. Changed-source native snapshots/
scans remain proportional to history, so these stable-poll measurements do not close issue #10.

[Raw monitored-poll results](incremental-monitored-sync-benchmark.json).

The final 1,000-extra-message end-to-end harness also passed for both adapters after the
watch-mode/schema changes, including encrypted isolated cold restore with original source and
cache directories moved offline. These smoke runs validate replay; their timings are not used
in the monitored-poll comparison above.

## WAL inspection component

On a synthetic 100,000-row SQLite database with an 11.18 MB WAL, initial WAL validation took
0.252 s. A no-change continuation read 88 logical bytes in 0.081 ms. An old-row edit, one new row
and a historical deletion each read one 4 KiB frame (4,208 total logical bytes including boundary
checks), taking 0.14–0.16 ms. These figures measure only the new WAL parser. They exclude SQL
mutation, native snapshot/read-view coordination, row/dependency selection, sync and archives.
They do not establish the acceptance case for changed-source sync: the parser is not yet used
by the adapters. Kernel storage I/O and energy were not measured for this microbenchmark.

[Raw WAL results](incremental-wal-benchmark.json) ·
[Remaining integration](incremental-preservation.md#native-wal-inspection-groundwork).

## Coordinated source-view component

Adding a pinned SQLite view and source identity guards to the 100,000-row WAL fixture measured
0.486 ms for a clean continuation, 0.421 ms for an old edit, 0.401 ms for a new row and 0.395 ms
for an old-row deletion. Initial WAL/view validation took 0.464 s. The changed cases each read
one WAL frame; metadata and WAL-index reads are additional to the WAL byte counters. Setup,
SQL mutation, row mapping, sync and archive work are excluded. This is a component benchmark,
not proof of end-to-end incremental changed-source sync or an energy measurement.

[Source-view counters](incremental-source-view-benchmark.json) ·
[Coordination guarantees and remaining work](incremental-preservation.md#pinned-source-views).

## Row-page decoder component

For a synthetic 100,000-row history, the baseline decoder mapped 100,000 row IDs by reading
2,711 owned pages in 0.236 s. After a same-length old-row edit, the selected-page/candidate-query
phase read one page, selected 37 candidate rows and found exactly one changed row in 0.0312 s.
Decoder reads were 4,196 logical bytes; SQLite candidate-query I/O is additional.

The fixture prepared two complete 11.11 MB private images, taking 0.0163 and 0.0167 s in this run.
Those copies are shown separately rather than hidden in the delta timing. Incremental image
maintenance and sync integration are not implemented, so this is not a claim that changed-source
sync has become bounded. WAL parsing, capture, archives, kernel storage I/O and energy are outside
this component measurement. The current production changed-source path still reconciles history.

[Row-page benchmark](incremental-pages-benchmark.json) ·
[Decoder guarantees and remaining integration](incremental-preservation.md#sqlite-row-page-decoding).


## Persistent page index component

The persistent SQLite index built ownership for 100,000 rows in 0.258 s. One old-row edit
selected 37 candidates and found exactly one changed row in 18.3 ms, including candidate SQL
queries and the index transaction. The decoder read three tree pages (12,388 logical bytes,
including its header read). Clean index continuation took 0.158 ms and read only the 100-byte
image header. Index metadata I/O is additional to those decoder counts.

Two full 11.11 MB images took 15.6 and 16.5 ms to prepare separately. These copies remain
required by this benchmark; this result does not demonstrate incremental native image
maintenance or bounded production sync. WAL validation, capture, archives, kernel I/O and
energy are outside the measured index stages.

[Persistent-index counters](incremental-page-index-benchmark.json) ·
[Ownership guarantees and remaining work](incremental-preservation.md#persistent-page-ownership).


## Coordinated WAL-backed row selection

The combined 100,000-row fixture now needs a full image only at baseline (0.599 s including
source coordination and index construction). Changed-point selection reads verified WAL
bodies and old index metadata, with no new full image. Including indexed candidate SQL reads,
an old edit took 5.46 ms, a new row 10.80 ms, an old deletion 4.89 ms and an overflow-only write
4.77 ms. Each retrieved one native page; the old edit selected 37 rows. A 50 KB payload change
visited six tree nodes and retrieved 18 WAL pages in 8.61 ms. Clean continuation took 0.633 ms,
with no candidate rows or page bodies.

These stages include WAL discovery and index transactions. The byte counters cover WAL
reads only; source/index SQLite and WAL-index I/O are additional. Setup and SQL mutation,
durable native mirror preparation, canonical extraction, sync, archives, kernel I/O and energy
are excluded. Production changed-source sync still scans history pending integration.

[WAL-backed selection counters](incremental-wal-image-benchmark.json) ·
[Guarantees and remaining integration](incremental-preservation.md#wal-backed-page-images).


## Durable native preparation component

For 100,000 synthetic rows, full baseline view/image/index/native-row preparation took
0.707 s. Subsequent durable PREPARED transactions took 6.19 ms for an old edit, 4.37 ms for a
new row, 5.79 ms for an old deletion and 6.04 ms for an overflow-only write. Each prepared one
changed native row and retrieved one WAL page. A 50 KB payload change took 9.15 ms and retrieved
18 pages. Clean preparation took 0.698 ms with no native rows changed.

The timed continuation stages include coordination, WAL validation, native-row comparison,
page-index/native-row/old-value commit and reading the resulting change set. No full database
image is made after baseline. The harness acknowledges points with synthetic receipt IDs so
it can measure later preparations; there is no canonical cache transaction in this benchmark.
Media bytes, canonical extraction, sync/archives, source mutation/setup, kernel I/O and energy
are excluded. SQLite/index/WAL-index I/O is additional to the WAL byte counters. This component
is not yet integrated into production sync.

[Native-mirror counters](incremental-native-mirror-benchmark.json) ·
[Crash/retry guarantees and remaining work](incremental-preservation.md#prepared-native-mirror).


## Explicit Messages selection

In a 100,000-message synthetic snapshot, full adapter extraction with private index construction
took 5.32 s and emitted 200,015 canonical records. An explicitly selected old-row edit took
6.69 ms (3.08 ms parent CPU plus 3.28 ms helper CPU), decoding one message and emitting 13 records
including context headers. A new-row selection took 6.18 ms; selecting a deleted row took 6.15 ms
and decoded no messages. Empty selection took 8.22 ms. The primary message query performed
zero full-scan steps in the selected cases, versus 99,999 in the full baseline.

A complete 24.20 MB private snapshot was prepared once in 46.8 ms. These timings include helper
launch, native SQL/body decoding, Python context reads, canonical construction and applicable
selected-row media probes. They exclude WAL/mirror preparation, automatic dependency routing,
canonical-cache writes, archives, source mutation/setup, kernel I/O and energy. Full-scan/VM
counters describe the primary query, not all helper SQL. Headers are still read completely and
unselected canonical freshness/deletions are not reconciled by this harness. This is an explicit
selection component measurement, not bounded production changed-source sync.

[Messages selection counters](incremental-messages-selection-benchmark.json) ·
[Selection guarantees and remaining work](incremental-preservation.md#selective-messages-decoding).


## Automatically routed Messages changes

The combined 100,000-message fixture measured 7.71 s for full baseline native preparation,
private indexes and adapter extraction. Subsequent WAL validation, durable native preparation,
automatic routing and selected extraction took 16.5 ms for an old edit, 14.3 ms for a new message,
10.7 ms for attachment metadata and 14.4 ms for an old deletion. The old edit read 8,640 logical
WAL bytes, routed one message and emitted 13 canonical records including context headers.
A chat-title change routed no messages and took 9.40 ms; clean continuation took 13.1 ms.

The old-edit stage used 11.7 ms parent CPU plus 4.34 ms helper CPU. No full source image is made
after baseline. These measurements still exclude canonical cache commits, retained-unit
freshness and targeted absence, durable media preparation, archives, source setup/mutation,
kernel I/O and energy. SQLite/index/WAL-index reads are additional to logical WAL counters.
The harness acknowledges points with synthetic IDs, not canonical receipts. Production sync
has not yet enabled selective canonical capture.

[Automatic routing counters](incremental-messages-dependencies-benchmark.json) ·
[Dependency guarantees and remaining canonical integration](incremental-preservation.md#messages-dependency-routing).


## Canonical summary and targeted withdrawal

For 100,000 cached messages, the previous summary query executed about 900,000 SQLite VM
instructions; the maintained-count/indexed-bounds query executed fewer than 100 at the probe's
resolution and returned the same count/date bounds. Instrumented CPU time was 16.5 ms versus
0.0860 ms in this run. Wall time for the scan included additional I/O/scheduling delay, so it
is not used as an end-to-end speedup claim.

With 100,000 unrelated unit memberships, withdrawing one unit's missing observation, including
journal triggers, memo invalidation and writer commit, took 0.623 ms wall / 0.595 ms CPU and about
400 measured VM instructions. Setup, source/mirror extraction, the complete selective-capture
lifecycle, archives, kernel I/O and energy are excluded. Timings include SQLite progress-handler
instrumentation; VM counts are floors at 100-instruction resolution. These are component
measurements, not a production selective-sync benchmark.

[Canonical coverage counters](incremental-canonical-coverage-benchmark.json) ·
[Summary/withdrawal guarantees and lifecycle work](incremental-preservation.md#canonical-summary-and-unit-withdrawal).


## Prepared Messages canonical lifecycle

The 100,000-message text-only run now includes WAL validation, durable native preparation,
automatic routing, selected extraction, canonical cache batches, retained-unit sightings,
targeted absence, coverage/checkpoint/receipt commit and native acknowledgement. The one-time
full baseline took 120 s wall and 67.2 s parent CPU in this run; this does not claim a baseline
speedup. Subsequent clean capture took 54.0 ms wall, an old edit 75.1 ms, a new message 59.8 ms,
a chat-title update 42.3 ms and an old deletion 53.0 ms.

The old edit emitted one message, updated its two canonical records and re-sighted 11 unchanged
headers, using 46.1 ms parent CPU plus 5.22 ms helper CPU. Deletion emitted no message and marked
one old observation absent while retaining its canonical content. Unselected message history
was re-sighted by coverage intervals. No full source image or full message traversal occurred
in those eligible continuation stages.

This benchmark uses actual canonical receipts and mirror acknowledgement. It excludes archive
creation/encryption, verification/import/delivery, file-backed media, WhatsApp, source setup and
mutation, kernel I/O and energy. WAL byte counters exclude SQLite/index/WAL-index reads. The
CLI collector now dispatches eligible Messages captures through this path; file-backed media
requires a retained watch session. Conservative fallback cases are documented. These component
measurements do not prove that the full issue is complete.

[Prepared canonical counters](incremental-prepared-messages-benchmark.json) ·
[Durability, eligibility and remaining work](incremental-preservation.md#durable-selective-messages-capture).

## Live media watcher resource limits

`scripts/benchmark_media_watch.py` creates synthetic files and measures only path-to-attachment
routing through the real macOS vnode monitor. Setup, file mutation, media retention, SQLite,
canonical sync and archive delivery are excluded. Clean values are medians of 100 polls;
sparse values are single polls after ten fsynced file writes. RSS is the lifetime process peak,
including fixture setup. Kernel I/O and energy were not measured.

| Files | Registration wall / CPU | Additional descriptors | Clean wall | Ten-file routing wall | Peak RSS |
| --- | --- | --- | --- | --- | --- |
| 1,000 | 117 / 117 ms | 1,009 | 27.9 µs | 67.2 µs | 33.7 MiB |
| 10,000 | 1,352 / 1,297 ms | 10,009 | 26.1 µs | 130.5 µs | 87.5 MiB |
| 100,000 | registration failed after 8.13 s | incomplete | unavailable | unavailable | 343 MiB |

The shell soft descriptor limit was 1,048,575, but `kern.maxfilesperproc` was 61,440 and
`kern.maxfiles` was 122,880. The 100,000-file case failed closed; it did not authorize clean
coverage. A separate run with a 256-descriptor process limit also failed closed after 27.7 ms.
Every run returned to its initial descriptor count after monitor shutdown.

These results establish bounded clean/sparse routing for the tested successful populations,
but expose a linear descriptor/memory cost and a hard large-history failure. The current
one-descriptor-per-file design does not satisfy large-population resource acceptance. A scalable
replacement must retain detection of writes through open handles, replacement, deletion and
late downloads; directory notifications alone do not prove unchanged file contents.

Reproduce with `.venv/bin/python scripts/benchmark_media_watch.py --files 10000`, then use
`--files 100000` or `--files 1000 --fd-limit 256` for the failure cases.
[Raw measurements and environment](incremental-media-watch-benchmark.json).

## Native WhatsApp collector, export and import

Run `.venv/bin/python scripts/benchmark_native_whatsapp.py --messages 100000 --aliases 1000 --pipeline`.
This uses the production collector, a retained auxiliary watch session, 100,000 synthetic native
message rows and 1,000 additional LID/phone pairs. File-backed media paths are cleared; the
fixture retains a not-downloaded attachment. The default export batch limit is 5,000 events.
Setup and mutations are outside timings. Each exported batch is encrypted and verified by the
exporter, then imported with trust-boundary verification into a separate empty destination.
The final assertion confirms the captured historical edit remains in both caches after deletion.

The initial baseline took 61.148 s wall / 48.701 s CPU for native/canonical sync, 83.753 / 76.976 s
for encrypted verified export, and 195.346 / 129.390 s for verified import. Export produced 102
immutable archives totaling 30,634,148 ciphertext bytes. Initial work is still substantial;
these timings are not a claim that baseline creation or cold import became faster.

| Subsequent operation | Sync wall / CPU ms | Verified export wall / CPU ms | Verified import wall / CPU ms | Ciphertext bytes |
| --- | ---: | ---: | ---: | ---: |
| Clean after baseline | 140.99 / 101.33 | 8.06 / 5.05 | 3.41 / 3.23 | 2,333 |
| Historical text edit | 798.30 / 257.32 | 6.60 / 6.24 | 8.00 / 5.63 | 3,381 |
| Chat title | 163.40 / 158.90 | 5.41 / 5.40 | 7.79 / 5.68 | 3,150 |
| Auxiliary alias edit | 168.02 / 161.96 | 5.57 / 5.51 | 7.14 / 5.42 | 3,251 |
| Historical deletion | 171.42 / 166.96 | 5.65 / 5.63 | 5.84 / 5.15 | 3,275 |
| Clean after changes | 48.25 / 47.05 | 5.21 / 4.92 | 4.52 / 3.65 | 2,402 |

Every subsequent export produced one archive. Header work for changed captures still visits
unrelated header rows, so these measurements do not prove proportional work as alias/header
cardinality grows. Clean prepared runs skipped extraction, as asserted by the harness.

The raw file includes Darwin process-attributed storage bytes and wakeups per stage. Baseline
sync reported 7.18 GB of writes, export 1.06 GB and import 22.33 GB: write amplification remains
an important audit target. The last clean sync reported 81,920 read bytes and 978,944 written
bytes. Cached reads can report zero and buffered attribution can cross stage boundaries;
these are not logical SQL byte counts. Lifetime process peak RSS grew from 166.7 MiB after
baseline sync to 287.6 MiB by the end. It includes fixture/baseline memory and is not an isolated
steady-poll memory measurement.

No network transfer, remote durability proof, file-backed media, new-message timing or energy
measurement is included. Ciphertext size is potential transfer volume, not observed traffic.
The run also exposed an oversized journal member when requesting 100,000 events; the exporter
now splits by encoded UTF-8 member size as well as event count, with regression tests for
gap-free sequences and oversized-single-event checkpoint retention.
[Raw production-path measurements](incremental-native-whatsapp-benchmark.json).

## Archive validation profile and exact-body deduplication

The 100,000-row run above predates archive-local schema deduplication. To identify its CPU
bottleneck, run the same production-path harness under cProfile with 10,000 message rows and
1,000 additional alias pairs. Before the change, 36 archive loads spent 34.0 cumulative seconds
parsing/validating records, including 29.6 seconds in 92,160 canonical JSON Schema checks.
Revision and current-head events often carried the identical canonical body.

The verifier now memoizes successful schema checks by a hash of the complete body within that
single parse. It still validates digests and envelopes for every occurrence. Complete-body
keys distinguish invalid provenance even when the claimed canonical digest is unchanged.
The memo is discarded between archive loads, so export and import perform independent checks.

| Profile counter | Before | After |
| --- | ---: | ---: |
| Canonical schema checks | 92,160 | 46,088 |
| Schema validation cumulative seconds | 29.60 | 16.43 |
| Archive parsing/validation cumulative seconds | 34.03 | 21.90 |
| Complete profiled run seconds | 50.04 | 40.69 |
| Baseline export CPU seconds | 18.83 | 11.97 |
| Baseline import CPU seconds | 22.09 | 16.21 |

Both runs used the same synthetic parameters with separate fixtures and full encryption,
digest, schema and import checks. These are cProfile-instrumented timings, not uninstrumented
performance estimates. Source/cache timing varied between runs, and the storage write
amplification remains unresolved. The 100,000-row production measurements should not be read
as post-optimization results. Tests verify fresh validation on another parse and rejection of
changed provenance, corrupted content and mismatched revision wrappers.

Reproduce the current profile with `.venv/bin/python -m cProfile -o /tmp/chatstore-wa.pstats
scripts/benchmark_native_whatsapp.py --messages 10000 --aliases 1000 --pipeline` on one line.
[Before/after counters and stage measurements](incremental-verification-profile.json).

## SQLite cache-spill experiment

The native WhatsApp harness accepts `--sqlite-stats` to build a temporary instrumentation
extension with the local C compiler. It samples the public `sqlite3_db_status` API on the
canonical producer and importer connections, then disables extension loading again. Normal
application connections do not load this extension. Native mirror and source-reader connections
are outside these counters. `cache_writes` counts pages written to WAL; `cache_spills` counts
pages written mid-transaction because the pager cache overflowed. Neither is a byte count or
a count of checkpoint writes. Cache bytes and file sizes are end-of-stage gauges.

Two independent 30,000-row fixtures with 100 additional alias pairs compared defaults with
`--disable-cache-spill`. Both used encryption, verified export/import and the same operation
sequence. This flag is benchmark-only; production settings were not changed.

| Baseline stage | Default WAL page writes / spills | Spilling disabled page writes / spills |
| --- | ---: | ---: |
| Producer sync | 186,980 / 20,786 | 186,410 / 0 |
| Consumer import | 271,456 / 0 | 271,489 / 0 |

Producer cache memory after sync grew from 78.4 MB to 90.0 MB. Attributed producer storage
writes were 1.316 GB versus 1.255 GB, while import writes were 3.754 GB versus 3.790 GB.
The import had no spills in either run, and disabling producer spills barely changed the
number of page writes. This does not support changing the production spill setting as a fix
for the observed amplification. Logical writes, checkpoint work and filesystem attribution
need further investigation.

These are individual runs, not randomized repeated trials; their wall times and lifetime peak
RSS also varied. The experiment rejects the broad spill hypothesis at this tested size, not
every possible large-transaction problem. No energy measurement is available here.

Reproduce with `.venv/bin/python scripts/benchmark_native_whatsapp.py --messages 30000
--aliases 100 --pipeline --sqlite-stats`, adding `--disable-cache-spill` for the second case.
[Raw counter comparison](incremental-sqlite-spill-benchmark.json).
