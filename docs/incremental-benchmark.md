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

That earlier run includes no network transfer, remote durability proof, file-backed media,
new-message timing or energy measurement. A later new-message run is recorded below. Ciphertext size is potential transfer volume, not observed traffic.
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

## Retained database layout

`--database-layout` adds a post-run `dbstat` survey and journal-operation byte counts to the
native WhatsApp harness. This inspection runs after all timed stages and reads database pages;
it is diagnostic work, not a collector operation. The 30,000-row / 100-extra-alias pipeline
completed with the default spill setting, full encryption and verified import.

| Object | Producer bytes | Importer bytes |
| --- | ---: | ---: |
| Journal (`changes`) | 182,681,600 | 182,681,600 |
| Current records | 82,432,000 | 82,432,000 |
| Retained revisions | 82,419,712 | 82,419,712 |
| Source observations | 11,313,152 | 11,313,152 |
| Applied-event ledger | absent | 20,017,152 |
| Producer observation-ID mapping | absent | 15,560,704 |
| Entire database page allocation | 471,031,808 | 502,992,896 |

Neither database had freelist pages. The journal alone occupies about 39% of the producer
database. Its revision events contain 69.64 MB of encoded data, head events 58.74 MB, and
observations 16.85 MB. Current heads, retained revisions and event payloads each carry canonical
body data. The earlier exact-body validation profile demonstrates duplicate head/revision
bodies, but the layout totals do not measure their exact deduplication potential: provenance
and historical-head metadata can differ even when canonical content digests match.

This points to logical payload/storage duplication as a more concrete next target than
disabling spills. A storage change must preserve expanded event bodies, producer sequence
hashes, independent consumer checkpoints, orphan recovery and historical provenance. No
journal rows, revisions or ciphertext were pruned, and no database tuning changed in this
inspection. Occupied bytes do not by themselves explain every attributed physical write;
checkpoint and filesystem work still need analysis.

Reproduce with `.venv/bin/python scripts/benchmark_native_whatsapp.py --messages 30000
--aliases 100 --pipeline --sqlite-stats --database-layout`.
[Full table/index layout, journal sizes and stage counters](incremental-database-layout.json).


## Isolated journal compression experiment

The journal layout survey motivated a per-event compression experiment. The script creates
2,000 synthetic WhatsApp messages through the real adapter and repeats their journal templates
ten times, producing 100,700 events. Repeated templates are not distinct source histories.
Each database uses FULL durability, a 64 MiB cache, 5,000-event commits and a final truncate
checkpoint. The compressed variant uses zlib level 1 only when it saves at least 64 bytes after
its length/checksum header. Small and incompressible events remain plain text.

| Measurement | Plain | Compressed |
| --- | ---: | ---: |
| Database allocation | 121,757,696 bytes | 64,233,472 bytes |
| Stored payloads | 96,543,160 bytes | 57,974,890 bytes |
| Write + checkpoint wall / CPU | 0.536 / 0.462 s | 1.722 / 1.680 s |
| Read + exact roundtrip wall / CPU | 0.173 / 0.173 s | 0.692 / 0.692 s |
| Write-stage attributed storage writes | 244,981,760 bytes | 129,683,456 bytes |

Compression reduced allocated journal storage by 47.2%, but write CPU rose 3.6 times and read
CPU rose 4.0 times. Both variants reconstructed identical operation names, sequence numbers
and exact original UTF-8 JSON text, with matching whole-stream SHA-256 hashes. Separate checks
rejected truncated headers/streams, trailing bytes, oversized or incorrect decoded lengths,
and checksum corruption; Unicode and plain-text roundtrips passed.

This measures an isolated table. It excludes production triggers, canonical tables and indexes,
archive creation, encryption, verified import, remote transfer and energy. It is one ordered
plain-then-compressed run, not a randomized comparison. Cached reads reported zero attributed
storage reads. Peak RSS includes template setup and preceding measurements. No production
cache encoding changed. The disk savings alone do not establish lower background cost;
integrating a codec requires end-to-end measurements and schema/recovery compatibility work.

Reproduce with `.venv/bin/python scripts/benchmark_journal_encoding.py`.
[Raw journal encoding measurements](incremental-journal-encoding.json).


## Release export buffers before verification

The exporter retained both its per-event encoded lines and their joined JSONL member after
writing the TAR. Both copies stayed live through encryption and authenticated verification.
It now clears the line list after joining and releases the joined bytes after writing the TAR
member. Recovery also clears its encoded lines before loading the existing archive. Parsed
events remain available for exact recovery comparison and attachment discovery.

A separate-process before/after run exported 200 synthetic messages with deterministic random
base64 text, 16,000 characters each, through real encrypted archive creation and verification.
The resulting batch contained 401 events. `tracemalloc` started immediately before export.

| Python traced allocation | Before | After |
| --- | ---: | ---: |
| Live at verification entry | 21,750,766 bytes | 8,183,464 bytes |
| Peak during the entire export | 46,315,855 bytes | 32,748,255 bytes |

Peak traced allocations fell 29.3%. These figures exclude native allocations and setup before
export; they are not process RSS or an estimate for every source history. This changes buffer
lifetimes only. Event bytes, authentication, archive retention and checkpoint order are
unchanged. The archive layer's 23 incremental integration tests passed, including cold restore,
publication recovery, byte bounds and consumer lineage.

Reproduce with `PYTHONPATH=src:. .venv/bin/python scripts/benchmark_export_memory.py`.
[Raw allocation measurements](incremental-export-memory.json).


## Parse record lines without a second member-sized list

Verification used `bytes.splitlines()`, which copied every line of a record member before
parsing the first record. It now iterates line matches and copies only the current line.
LF, CRLF and CR separators retain their previous behavior, including blank-line handling and
line numbers in invalid-JSON errors. All row, schema, digest and feed-contiguity checks still run.

Using the same 200-message / 401-event encrypted export allocation benchmark, peak Python-traced
allocations fell from 32,749,076 to 25,972,311 bytes, another 20.7%. The baseline already includes
the export-buffer lifetime fix above. Live allocations at verification entry were about 8.18 MB
in both runs; the saving occurs during parsing. Six new compatibility cases cover each line
separator with and without a final separator, Unicode/escaped text, blank lines and malformed
JSON. The focused incremental and record-validation suites passed all 33 tests.

This remains a single synthetic allocation comparison, not a process RSS, CPU or energy result.
It uses the same `scripts/benchmark_export_memory.py` reproduction command. Raw allocations
include only work after export starts and exclude native-library allocations.
[Raw record-line allocation measurements](incremental-record-line-memory.json).


## New-message pipeline on 100,000 native WhatsApp rows

The production WhatsApp benchmark now inserts one new native message after baseline and clean
capture, then checks another clean poll. It asserts incremental mode, one emitted message and
one rebuilt canonical unit for the insert. Final producer/importer checks require the new
message and the captured historical edit after source deletion. Export output now includes
journal-event counts as well as archive count and ciphertext size.

The run used 100,000 starting native messages, 1,000 extra alias pairs and 5,000-event archive
batches. It includes the schema-validation and buffer-lifetime changes above. Baseline sync
took 46.490 s wall / 41.175 s CPU, encrypted verified export 52.132 / 49.896 s, and verified
import 101.496 / 91.929 s. The baseline carried 509,109 events in 102 archives totaling
30,637,473 ciphertext bytes. This is a later single run, not a controlled speedup comparison
with the older baseline.

| Operation | Sync wall / CPU ms | Verified export wall / CPU ms | Verified import wall / CPU ms | Export events | Ciphertext bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| Clean after baseline | 113.09 / 93.15 | 5.12 / 4.42 | 3.17 / 2.93 | 6 | 2,337 |
| New message | 710.73 / 207.67 | 6.09 / 5.28 | 4.56 / 4.51 | 19 | 3,303 |
| Clean after new message | 45.97 / 44.73 | 4.55 / 4.50 | 2.88 / 2.86 | 6 | 2,377 |
| Historical edit | 165.59 / 156.12 | 5.59 / 5.47 | 5.21 / 4.90 | 19 | 3,401 |
| Chat title | 148.56 / 145.13 | 5.45 / 5.28 | 4.23 / 4.13 | 19 | 3,166 |
| Auxiliary alias edit | 155.19 / 148.00 | 5.68 / 5.59 | 4.85 / 4.74 | 21 | 3,264 |
| Historical deletion | 156.45 / 153.19 | 4.94 / 4.89 | 3.63 / 3.40 | 14 | 2,679 |
| Clean after all changes | 45.98 / 44.04 | 4.52 / 4.49 | 2.95 / 2.82 | 6 | 2,414 |

Each subsequent export wrote one archive. Clean polls preserve new freshness evidence, so their
exports contain six metadata events rather than zero events. The new-message capture emitted
one message unit but still visited 2,007 identities, 1,002 aliases and other headers; 3,020
canonical writes were reported unchanged. Incremental header work remains open.

The new-message sync reported 91,561,984 attributed read bytes and 2,654,208 written bytes.
The following clean sync reported 86,016 reads and 929,792 writes. The first changed capture's
read volume needs investigation; small output does not establish bounded source I/O. Darwin
counters can attribute buffered work across stage boundaries, so these are not logical query
bytes. Baseline sync/export/import writes were 7.20 / 1.06 / 24.70 GB. Write amplification
remains substantial. Lifetime RSS reached 321.0 MiB after the new-message capture; this is not
an isolated incremental peak or evidence that whole-process RSS improved with the allocation
changes above.

Setup and source mutations are outside timings. The new row has a unique native key and a
source date after the fixture's latest message. No network traffic, remote proof, file-backed
media or joules are measured. Run the command at the start of the production-path section to
reproduce the expanded stages. Both the large pipeline and a fresh 100-row smoke run completed;
the latter also ran the final new-message count assertions.
[Raw expanded pipeline measurements](incremental-new-message-pipeline.json).


## Diagnose first changed capture reads

A new diagnostic wrapper profiles only the new-message capture and installs 1,000-step SQLite
progress callbacks on its canonical Cache and newly opened SQLite connections. It leaves the
production collector unchanged. Two no-pipeline runs held the alias count fixed at 1,000 extra
pairs while increasing starting message rows from 100 to 100,000.

| Instrumented new-message capture | 100 rows | 100,000 rows |
| --- | ---: | ---: |
| Wall / CPU seconds | 0.250 / 0.248 | 0.508 / 0.292 |
| Canonical SQLite VM steps, rounded down | 622,000 | 632,000 |
| Native-mirror VM steps, rounded down | under callback interval | 26,000 |
| Rebuilt message units | 1 | 1 |

Canonical VM work rose about 1.6% for 1,000 times as many messages with the same headers. This
supports bounded logical canonical work for this fixture shape. It does not establish bounded
physical I/O, auxiliary/header cardinality scaling or arbitrary source schemas. VM counts omit
partial 1,000-step intervals per connection and do not count Python WAL/page decoding.

In the larger profile, routing took 0.46 ms. Canonical current-head lookups took 133.9 ms and
header withdrawal 53.2 ms. Extraction took 102 ms, and four code-signature computations took
72 ms cumulatively; these figures overlap other parent frames and must not be summed as
independent stages. The capture still emitted 3,022 records because unrelated headers were
materialized. The no-pipeline large run reported 49.2 MB of attributed reads, compared with
91.6 MB in the preceding encrypted-pipeline run. This variation and the profile point toward
canonical header lookup/cache behavior as the next target; they do not prove where every
physical read originated or excuse the observed volume. No production optimization follows
from this measurement alone.

Reproduce with `PYTHONPATH=src:. .venv/bin/python scripts/benchmark_whatsapp_new_profile.py
--messages 100000 --aliases 1000`, then repeat with `--messages 100`. The wrapper accepts the
underlying benchmark's CLI options; pass `--pipeline` only when that additional work is wanted.
Its timings include profiling overhead. A fresh small run validated the durable wrapper.
[Raw profiles and VM counters](incremental-new-capture-profile.json).


## Message-only header reuse measurement

After adding the indexed header-input proof, the same instrumented no-pipeline benchmark used
100,000 starting messages and 1,000 extra alias pairs. The new-message capture emitted one
message unit and two new canonical records, with zero unchanged emitted records. It skipped
the previous 3,020 unchanged header records and frozen alias JSON loading.

| Instrumented new-message measurement | Before header reuse | After |
| --- | ---: | ---: |
| Wall / CPU seconds | 0.508 / 0.292 | 0.112 / 0.098 |
| Canonical SQLite VM steps, rounded down | 632,000 | 8,000 |
| Native-mirror VM steps, rounded down | 26,000 | 26,000 |
| Attributed read bytes | 49,201,152 | 1,433,600 |
| Attributed written bytes | 2,752,512 | 2,478,080 |

The callback counts show that canonical work dropped with header materialization removed.
The physical counters are single-run process attribution and remain sensitive to cache state;
the reduction is not a universal I/O or energy guarantee. Both captures include cProfile and
SQLite-progress overhead. Baseline work is still full-history work. Code-signature computation
now accounts for 74 ms cumulatively in the larger capture profile; parent frame timings overlap.

The large diagnostic completed all stages. A separate 100-message / 1,000-extra-alias encrypted
pipeline smoke also passed capture, export and verified import after the change. It is a
correctness smoke, not a replacement for the previous large pipeline timings. The tests add
known/unknown JIDs, owner-set changes, shared-row removal, offline interrupted retry, cross-chat
reply routing and encrypted cold restoration of header freshness. All 511 cumulative tests,
Ruff and mypy passed. Large end-to-end media/remote/energy acceptance remains incomplete.

Reproduce with the new-capture profiler command above. Raw results include
[the 100,000-message profile](incremental-header-reuse-profile.json) and
[the small encrypted pipeline](incremental-header-reuse-pipeline-smoke.json).


## File-bearing WhatsApp history through the encrypted pipeline

`benchmark_native_whatsapp.py --file-media` retains a source attachment path and supplies the
persistent media watcher. The fixture file has 256 KiB of deterministic random bytes. File-only
edit, missing-file, re-download and clean-after-file stages run in addition to the existing
message/context cases. They assert one selected message and retained header coverage.
`--archive-media available-media` can additionally include retained attachment bytes; the
ordinary benchmark default remains text-only.

A 100,000-message / 1,000-extra-alias text-only pipeline completed with one watched file.
Baseline sync took 52.525 s wall / 45.135 s CPU, encrypted verified export 54.665 / 51.863 s,
and verified import 127.155 / 96.402 s. The baseline exported 509,111 events in 102 archives,
30,631,393 ciphertext bytes. Initial history work remains substantial.

| Operation | Sync wall / CPU ms | Verified export wall / CPU ms | Verified import wall / CPU ms | Export events | Ciphertext bytes |
| --- | ---: | ---: | ---: | ---: | ---: |
| clean-after-new | 48.55 / 46.12 | 4.59 / 4.31 | 3.07 / 2.72 | 6 | 2,369 |
| new-message | 66.11 / 54.27 | 4.92 / 4.88 | 4.50 / 3.88 | 12 | 3,181 |
| old-edit | 66.68 / 56.29 | 4.94 / 4.89 | 6.74 / 4.43 | 12 | 3,277 |
| file-only-edit | 55.42 / 49.60 | 4.93 / 4.79 | 4.32 / 3.88 | 14 | 3,103 |
| clean-after-file | 48.08 / 46.29 | 4.09 / 4.07 | 2.93 / 2.84 | 6 | 2,420 |
| file-missing | 53.66 / 51.12 | 4.70 / 4.54 | 4.67 / 4.45 | 12 | 2,942 |
| file-download | 52.30 / 48.78 | 4.75 / 4.71 | 4.74 / 4.52 | 14 | 3,122 |

For the file-only edit, sync reported 647,168 read bytes and 2,080,768 written bytes; these
include retention of the new file version. The clean-after-file sync reported 102,400 reads
and 1,036,288 writes. Counters are Darwin process attribution, not logical query or device
bytes; buffered attribution can cross stage boundaries. Lifetime RSS ended at 299.7 MiB and
includes baseline/setup, so it is not an isolated incremental peak. No network or joules were
measured. This validates one watched file, not the unresolved large-file-population descriptor
ceiling. Genuine header/auxiliary changes still materialize headers.

A separate 100-message / 10-extra-alias pipeline included available media and verified the
final imported blob bytes. The native media integration tests additionally cold-restore all
three byte versions and four attachment revisions after source and producer blobs disappear.
An isolated CLI check performs two watch polls at the configured 900-second cadence, replacing
the sleep for verification, and checks the second file-only change has no SQL row changes.

Reproduce the large text-only run with `.venv/bin/python scripts/benchmark_native_whatsapp.py
--messages 100000 --aliases 1000 --file-media --pipeline`. Add `--archive-media available-media`
for attachment bytes; the recorded byte-including smoke used 100 messages and 10 aliases.
Run `PYTHONPATH=src:. .venv/bin/python scripts/verify_whatsapp_media_watch.py` for the CLI check.
The cumulative suite passed 525 tests, Ruff and mypy.

[Large file-bearing text-only pipeline](incremental-whatsapp-file-media.json),
[byte-including pipeline smoke](incremental-whatsapp-media-archive-smoke.json),
[CLI verification](incremental-whatsapp-media-cli.json).


## WAL reset correctness smoke

`--wal-resets` adds clean and historical-edit reset stages to the native WhatsApp benchmark.
[Raw results](incremental-wal-reset-smoke.json) use 100 messages, 10 alias pairs and one
watched 256 KiB file with text-only archives. Both reset stages complete encrypted export
and verified import. Checkpoint/truncate and fixture mutation happen outside capture timing;
full source image backup, page ownership rebuild and ordered raw comparison happen inside it.
This does not establish bounded discovery or large-history reset performance.


## Energy counter availability

On this laptop, `powermetrics --samplers tasks,cpu_power --sample-count 1
--sample-rate 100` exits with `powermetrics must be invoked as the superuser`.
This run therefore has no direct energy measurement. CPU time and process wakeup
counters are reported separately and must not be relabeled as joules or Activity
Monitor energy impact. No system privilege or sampling configuration was changed.


## Large-history WAL reset measurement

[Raw results](incremental-wal-reset-large.json) run the production collector with
100,000 messages, 1,000 extra alias pairs, one watched 256 KiB file, and encrypted
text-only export/verified import. Initial capture/export/import took 59.955 /
59.110 / 229.448 seconds wall time and 48.163 / 55.350 / 118.171 seconds CPU.

Compatible clean WAL reset capture took 1.617 seconds wall / 1.327 CPU; a reset
with a historical edit took 1.263 / 1.187 seconds. These include full image backup,
page ownership rebuild and ordered raw comparison. Their export/import stages
took 4–10 ms each. The following ordinary historical edit took 67 ms capture,
6 ms export and 6 ms import. A final clean capture took 56 ms.

The measurements establish a bounded canonical delta after compatible resets,
not bounded source discovery. File monitoring has only one fixture file. Wall
times include contention and vary from earlier runs; process CPU and attributed
I/O are reported separately in the raw output. RSS remains the lifetime peak.
This run has no real remote-host transfer or direct energy measurement.

The HTTPS delivery integration test uses a local TLS server trusted through a
temporary test CA. A truncated archive response leaves the delivery checkpoint
unadvanced; retry and idempotent reproof retrieve metadata and actual ciphertext
again over TLS. This is protocol/failure evidence, not remote throughput evidence.


## Unchanged auxiliary evidence

[incremental-auxiliary-noop.json](incremental-auxiliary-noop.json) adds an optional
database `user_version` commit without changing alias rows. With 1,000 messages and
1,000 extra alias pairs, capture takes 66 ms wall / 59 ms CPU and reuses canonical
coverage. Verified export/import take 5 / 3 ms. A following real alias edit takes
284 ms wall / 219 ms CPU and rebuilds headers. Full optional-source evidence is
still read on a changed database; the comparison only avoids unnecessary canonical
materialization. This fixture is a focused check, not large-history acceptance.
