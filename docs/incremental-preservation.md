# Incremental preservation

`archive export --consumer NAME` exports immutable, bounded changefeed batches. The first
batches form a baseline; subsequent batches contain changes and compact freshness receipts.
Monthly exports remain available without `--consumer`. The changefeed format requires this
version of chatstore on both machines.

```sh
chatstore --json --quiet sync --source whatsapp
chatstore --json --quiet sync --source messages
chatstore --json --quiet archive export --consumer linux --media text --output /private/batches
```

Repeat the export while `data.has_more` is true. The default limit is 5,000 journal events per
invocation; `--batch-size` changes it. This bounds collection and verification memory, including
the initial baseline. There is no date filter or separate catalogue: historical changes and
context changes share the journal. `--consumer` rejects date, catalogue, minimal-context and
force options. Changing media policy requires a new consumer and its own baseline.

Export is a local durability checkpoint, separate from upload/delivery. Keep the SQLite cache,
its WAL and all committed batches until delivery succeeds. Discover committed pending work,
including after losing an export command's stdout, with:

```sh
chatstore --json archive pending --consumer linux --destination linux-backup
```

Each entry contains the archive path and its verification receipt. Upload the immutable archive
and publish that receipt as a separate JSON object. The receipt binds the export ID, producer,
stream, sequence interval, predecessor ID and predecessor ciphertext hash, payload digest,
ciphertext hash and size, and the checks performed. A trusted caller can use the receipt and a
ciphertext hash comparison instead of immediately decrypting/verifying the same local archive
again. Receipts are local verification evidence, not signatures or remote durability proofs.
A receiver still authenticates/decrypts each new archive at its own trust boundary.

A destination acknowledgement requires fresh HTTPS retrieval of **both** published metadata and
ciphertext, even when the local archive/cache already exists:

```sh
chatstore --json archive confirm-delivery --destination linux-backup \
  --export-id urn:uuid:EXPORT-ID \
  --receipt-url https://backup.example/batch.receipt.json \
  --archive-url https://backup.example/batch.zip
```

The published JSON must exactly match the committed local receipt. Retrieved bytes are streamed
through SHA-256 and compared with the verified hash and length. Exact ciphertext identity links
those bytes to the local authenticated verification, without another decryption pass. No-cache
request headers are sent; the HTTPS service remains responsible for its actual storage semantics
and for serving origin data. This command cannot prove a provider's internal replication or
promise future retention. Failures, truncated responses and missing predecessors leave the
independent destination checkpoint unchanged. Repeated proofs fetch both objects again.
Authentication available through HTTPS URLs/normal urllib handling is supported; this is not
an uploader or a provider-specific object-store client.

## Change capture and recovery

SQLite triggers journal current-head changes, newly captured revisions (including non-current
conflict revisions), reversion metadata and changes to observation evidence. This includes
identities, aliases, context, tombstones and attachment availability. A transaction rollback
rolls back its journal events too. Existing caches migrate once, seeding **all** retained
revisions, heads and observations. New consumers replay that journal independently.

Unchanged sightings do not create revisions or repeat observation bodies. A sync batch emits
ranges of stable entity and observation IDs with their exact observation time/run. IDs are never
reused. Both the collector and importer store disjoint latest-sighting intervals; adjacent
intervals with the same time/run coalesce. Replacing that projection metadata does not remove
historical journal events or ciphertext. Resolution, head-ordering checks and monthly exports
read effective freshness instead of rewriting every canonical/provenance row.

A receiver binds each entity ID to the producer's last head digest, and each observation ID to
the producer's exact evidence. Refreshes apply only when the current local head/evidence still
matches; they cannot incorrectly refresh an unrelated local edit or changed provenance. Unknown
producer keys reject the batch without advancing the import checkpoint. Existing feeds migrate
entity keys and latest head times without altering older journal sequences. Cache readers combine
stored content metadata with sighting intervals; raw SQLite rows are a projection, not the
canonical interchange contract. Metadata changes to identity scopes/mappings are captured during export, and
scope-map commands capture their mapping in the same SQLite transaction as their aliases.

Sync commits a `running` record before source extraction. Every partially committed capture
therefore has a durable run reference if the process dies. Only a complete run commits its
adapter checkpoint, in the same transaction as the final run record. Failed/partial extraction
leaves the prior adapter checkpoint. Source absence is provenance evidence; it does not erase
captured text or manufacture an explicit source deletion.

Each stream batch authenticates a contiguous `(after, through]` sequence and names the exact
predecessor ciphertext hash. Lineage metadata has constant size. Files are verified, fsynced,
and their directory fsynced before the export ledger, receipt and consumer checkpoint commit.
The SQLite writer lock serializes publication with sync and other exporters for one bounded
batch. If publication succeeds but the transaction does not, retry verifies that exact file,
compares its events to the journal slice, and commits it without rewriting the ciphertext.
If a committed file is lost, recover it from its destination or start a new consumer; a consumer
checkpoint is never silently rewound.

Import authenticates container, member checksums, canonical revision bodies, counts and feed
shape before applying anything. Parent checking, head/revision replay, observation mappings,
freshness ranges and the import checkpoint share a transaction. Reusing an export ID with
different ciphertext is rejected. Producer event hashes also prevent overlapping consumers from
replaying the same captures or a divergent clone from reusing a journal sequence. Divergent local heads remain conflicts with their incoming
content retained as revisions. Objects never retire omitted records and never supersede older
ciphertext.

Cold restore needs only the encrypted changefeed archives and password:

```sh
chatstore --data-dir /isolated/restore --json archive import /downloaded/batches
```

Directory input sorts generated filenames into stream/sequence order. The initial baseline and
**every** subsequent batch are required; importing a later batch first fails without advancing
the import cursor. Several streams may be restored independently. Source apps, their databases,
export receipts and the original cache are not required. Importing the same object again is
idempotent and can restore missing carried media. Text-only retries hash the exact ciphertext
against the committed import verification before skipping redundant decryption and parsing; new
or changed bytes must cross the full authentication boundary. Text-only exports preserve attachment metadata,
not attachment bytes. `available-media` stops without checkpoint advancement if bytes promised by
a captured available attachment cannot be located at export time.

No consolidation or automatic pruning is introduced. The journal, archives and captured
superseded/deleted content are retained. Explicit `purge` removes the cache projection; it does
not erase prior journal entries or ciphertext.

## Source limits and remaining work

This implements the incremental **cache → archive → verification → import/delivery** path of
[issue #10](https://github.com/lukeramsden/chatstore/issues/10). It does **not** satisfy the entire
issue's bounded-work requirement for native source sync. One-shot sync still snapshots and scans source history. Stable polls within
`sync --watch-interval` can now reuse monitored capture coverage as described below. WhatsApp's recent-row window was removed because it missed historical edits,
backfills, disappearance and attachment changes. Apple Messages continues decoding all rows;
read receipts, tapbacks and edits can mutate old rows. Fully scanned tables reconcile absence
on every poll, including tables that become empty.

The snapshot/read/decode costs remain measurable and can dominate after export is made cheap.
Import translates received sightings through compact producer-to-local intervals. Semantic
local changes and mapping updates queue only affected keys for revalidation; clean sightings
read intervals without enumerating covered entities or observations. Fragmented mappings can
require multiple intervals. Older caches build the projection once on first use. No source change cursor is claimed where the source provides none. A future source
optimization needs independently tested historical row/dependency change detection, filesystem
media changes, snapshot/reset handling and transactional capture before it can replace these
checks. Compact local sightings remove most poll writes; native extraction memoization now avoids
canonical reconstruction for unchanged message groups, but native scanning remains proportional
to history. A timestamp or maximum row ID alone is insufficient. No poll interval or retention was
reduced. Content created and removed between source snapshots cannot be recovered; neither can
content that was already absent before the first capture.

## Measurements

Run the synthetic benchmark in a fresh directory:

```sh
.venv/bin/python scripts/benchmark_incremental.py --messages 100000
```

It creates made-up WhatsApp history spanning roughly six years, runs each measured stage in a
fresh process and tests cold import with no source access. Output reports wall/CPU time, peak
RSS, archive bytes, kernel storage read/write counters and macOS wakeups where available.
See [the measured 100,000-message run](incremental-benchmark.md) and [raw counters](incremental-benchmark.json).

Kernel storage counters include caching/writeback effects and are not logical SQLite byte
counts. Archive bytes are transfer size, not a measured network upload. Energy joules are null;
wakeup counts are not an energy benchmark. The harness excludes imports/configuration/Cache
construction from stage timing but includes their memory in peak RSS. Do not treat these results
as a measurement of the user's private history or real server/network performance.

## Native extraction reuse

Each logical message group has a transactional memo containing its native-input fingerprint,
canonical entity and observation ranges, and output counters. Fingerprints include dependent
native rows, identity/context references and full media-byte hashes or availability. A same-size
media replacement is checked even when the database is unchanged. The memo also binds adapter
code, canonical policies, configuration and runtime; policy changes rebuild affected output.

Semantic canonical or provenance changes and local purges invalidate dependent memos. Pending
reuse is revalidated under the writer lock before applying exact sighting ranges. New canonical
records and memo metadata commit together. Failed units remain uncached; oversized groups use
bounded capture batches without reuse. Full sync bypasses reuse and rebuilds its dependency
projection. Counters distinguish `canonical_units_rebuilt` and `canonical_units_reused`.

These counters describe canonical construction, not native decoding. WhatsApp still reads all
message groups and Messages still invokes the full-history Rust decoder. One-shot and changed-source polls still snapshot source databases and check referenced media
bytes. Stable monitored polls reuse prior capture only while live dependency coverage holds. No timestamp-only shortcut or
reduced polling cadence is used.

The harness also accepts `--source messages`. Current results separate parent and helper CPU,
report helper peak RSS and a conservative sum of process peaks, and explicitly scope storage
byte counters to the parent process. Cache close/checkpoint is included in stage timing.
Cold-restore stages move the synthetic source and original cache directories offline while
importing the encrypted archive sequence into a new destination.

## Imported sighting intervals

`import_key_ranges` maps contiguous producer IDs to local IDs using an offset and an eligibility
flag. Eligibility binds a record to its captured head digest or an observation to all captured
identity/evidence fields. Local semantic mutations, purge and mapping updates queue affected
keys in `import_range_dirty`, using reverse mapping indexes. Updating those points splits or
merges intervals without rechecking unrelated keys. Every import drains its producer's queued
points in the same writer transaction as journal replay and the import checkpoint. Baseline
batches build this projection incrementally; an upgraded older cache builds it once.

Range translation checks complete producer-key coverage, including ineligible keys, before
stamping eligible local intervals. Missing keys reject the archive and roll back its changes.
Divergent local heads/evidence stay ineligible until matching evidence is restored or imported.
Mappings need not preserve order or be one-to-one; translated intervals merge overlaps.
Fragmentation costs scale with the required mapping/eligibility intervals, not their numeric
width. A clean import test denies SQL reads of individual mapping and canonical tables.

## Why a filesystem-history shortcut was rejected

The synthetic macOS probe in `scripts/diagnose_fsevents.py` opens a file, writes through the
same still-open handle, calls `fsync`, and queries historical FSEvents with a new stream for
each write. On this host, all five queries delivered HistoryDone and completed a synchronous
flush without reporting those writes. A persistent `EVFILT_VNODE` registration reported each
write. A retained read-only SQLite connection also detected every committed historical-row
update through `PRAGMA data_version`.

[Recorded diagnostic](incremental-fsevents-diagnostic.json). Reproduce on macOS with
`.venv/bin/python scripts/diagnose_fsevents.py`. It creates only temporary synthetic files.
This is a counterexample to treating a drained filesystem history as proof of unchanged media,
not a claim that FSEvents never reports content writes. The diagnostic is not used by sync.
Native scans and media hashing remain enabled.

A bounded source fast path therefore needs a monitor that stays alive between polls, retaining
SQLite connections and write notifications for referenced files. Its session must be bound to
capture state, and monitor loss/restart, path replacement, notification errors or unregistered
media must force reconciliation. A persisted event cursor alone is insufficient. Database
change detection still needs a separate strategy for selecting historical rows/dependencies
when commits occur; detecting a commit does not identify its changed messages. The live monitor is now implemented and integrated as described below; native changed-row
selection remains open, and issue #10 remains incomplete.

## Live monitor component

`adapters/monitor.py` now provides a process-local `SourceMonitor`. It retains read-only SQLite
connections without open read transactions and vnode registrations for referenced media and
existing path ancestors. An unchanged poll drains queued notifications and queries one
`PRAGMA data_version` per database; it does not stat/open every media path. The component has
no durable cursor and never accepts tokens from another session.

The caller takes a token before capture, registers each dependency before reading its bytes,
and acknowledges that token only after successful capture. A write during capture, newly
registered dependency or changed file identity prevents acceptance. Registration changes need
a further capture with stable coverage. Notifications stay unacknowledged across repeated
polls. Database replacement invalidates the whole monitor, since retained SQLite handles still
refer to the old inode. Media replacement requires rearming the path; existing parent watches
also detect previously missing media being created. Symlinks and descriptor/notification errors
fail closed. Descriptor use grows with registered paths and ancestors; exhausted resources must
fall back to reconciliation, not reduce coverage.

Tests exercise open-handle writes, historical WAL commits, replacement/rearming, missing-file
creation, cross-session tokens, resource failure and closed monitors. An unchanged-poll test
forbids stat/open calls after registering 100 media files. The component is now connected through `SyncSession` and CLI watch mode. One-shot sync and
changed/untrusted monitored sources continue using native scans and media hashing.

## Persistent sync lifecycle

Run `chatstore --json sync --watch-interval 900` to retain monitors between 15-minute polls.
It writes one flushed JSON envelope per poll. `--max-polls N` bounds a run for diagnostics;
`--mode full` always captures. The command does not install a service, change source apps,
upload archives or reduce the requested capture cadence. Archive delta export can run
independently using its existing writer lock and consumer checkpoints. Existing one-shot
wrappers keep their current behavior until explicitly switched to a persistent sync process.

A stable session skips source snapshots and adapter extraction, reusing exact coalesced entity
and observation ranges from a successful capture. It journals new sightings and a complete
sync run. Aggregate counts/coverage are reused while the semantic cache generation matches;
the fast path does not recount the entire message index. A transactional projection generation per source/account scope
changes on its semantic records/provenance writes and purges, excluding sync-run/freshness writes.
Intervening cache writers during capture prevent publication of a reuse summary.

Sessions bind source/scope, cache path, configuration/code policy, working directory and Messages
helper ciphertext-free binary SHA-256. The helper hash is checked per poll, a constant cost
independent of retained history; watching its attribute events proved noisy on execution.
Code signatures use deterministic code/constant representations rather than marshal's runtime
reference encoding. Media is registered before byte probing; initially discovered registrations
require another stable capture. Symlinks, resource limits, database replacement, monitor loss
and unsupported platforms fall back to full capture. Restart never trusts an old monitor token.

This satisfies the stable monitored no-change case, not the whole native incremental problem.
A changed database still causes a full native snapshot/scan, even when only one old row changed;
canonical construction then reuses unchanged groups. Selecting native changed rows/dependencies
without rescanning remains open, alongside full final acceptance and operational integration.

Reuse summaries combine indexed rows written by the current capture with its coalesced sighting
intervals. They do not rescan retained canonical JSON to reconstruct coverage. A cached coverage
aggregate is reused only while the semantic projection generation still matches. Directory
ancestor watches detect identity/access changes; child-creation watches are needed only at the
nearest existing parent of missing dependencies. This avoids unrelated ancestor-directory churn
forcing every stable poll to rescan.

### Independent monitored sources

Reuse checks `source_projection_epochs` by source and account scope. A capture or import for a
different source/scope no longer forces unrelated monitored sources through native scans.
Record/provenance moves invalidate both their old and new namespaces. Edits, insertions,
provenance changes and purges advance the corresponding generation in the same transaction;
rollbacks restore it. Freshness-only and sync-run writes do not advance it.

The former global projection counter remains updated for watch processes that were already
running the previous code when a newer process opened the cache. New sessions use the scoped
counter; older sessions remain conservative and safe until restarted. Tests cover actual
cross-adapter capture/reuse, scope isolation, provenance changes, rollback, purge and namespace
moves. This narrows cache invalidation; it does not select native changed rows after a source
commit, which remains unfinished.

Manual aliases can have no native source/scope. They remain transactional journal events and
invalidate older global-counter sessions, but do not create a native reuse namespace. Their
updates are exported independently; source captures do not claim to re-observe manual aliases.
Scoped-trigger upgrades are atomic and advance stored scoped generations once. Reopening
an already upgraded cache does not invalidate them again.

## Native WAL inspection groundwork

`adapters/wal.py` validates SQLite WAL headers, salts, frame checksums and committed boundaries,
returning changed page numbers and a candidate cursor. It reads only frames after a previously
validated cursor; a clean continuation reads the header and prior boundary rather than the log's
history. Buffers are bounded to one page/frame; the changed-page set scales with the inspected
batch. The default frame limit rejects oversized batches for reconciliation.

Valid uncommitted tail frames do not advance the candidate. Partial/invalid frames, missing logs,
replacement, reset or broken continuity reject the candidate. An optional committed frame bound
can exclude a stale/preallocated suffix, but the caller must obtain that bound from a coordinated
source read view. A commit appended beyond the sampled extent remains after the returned cursor
and is found by the next read. Checksums detect WAL damage; they are not archive authentication.

This reader is not wired into sync and does not advance source/export checkpoints. WAL bytes
alone do not prove which SQLite read view was captured. The source-view coordinator below now binds candidates to SQLite snapshots. The remaining
implementation needs a page-to-row/dependency index (including overflow, splits and
truncation), and bounded adapter extraction with exact disappearance/re-sighting semantics.
Restart, log discontinuity and unsupported source shapes must retain the full-capture fallback.
Changed-source polls still scan history until those pieces are implemented and verified.

Tests use real temporary SQLite databases, including 512-byte and 64 KiB pages, historical edits,
deletes, older-row insertion, uncommitted spill, reset, corruption and concurrent append during
read. Reproduce the parsing microbenchmark with
`.venv/bin/python scripts/benchmark_wal.py --rows 100000`.
[Raw parsing counters](incremental-wal-benchmark.json). The counters exclude source-view
coordination, SQL mutation, adapter work, capture and archive I/O; they are not end-to-end sync
measurements or energy measurements.

## Pinned source views

`adapters/source_view.py` provides `WalSource.view(after)` on macOS. It pins a read-only SQLite
transaction and binds the WAL candidate to that view's committed frame, page size/count, salts,
frame checksum and checksum byte order. Two matching WAL-index header copies, their checksum,
and repeated header reads reject torn/changing headers. A retained SQLite/vnode monitor rejects
commits or database identity transitions during pinning, including rename-and-restore transitions
that leave the same inode at the path afterward.

After pinning, newer commits may proceed without changing the yielded SQL view. They stay beyond
its candidate cursor and appear in the next read. WAL-index `mxFrame` supplies an explicit committed
extent, excluding uncommitted spill and stale preallocated suffixes. Schema version is read inside
the pinned transaction for later dependency/index validation. Source cursors bind both database
identity and WAL continuity; an old cursor from a replacement database/log is rejected.

The reader lock lasts until context exit, including exceptional exit. Consumer exceptions
propagate without being reclassified as source errors. The coordinator never adopts a cursor or
advances a cache/export watermark. A caller must publish it only after durable capture. A view opened without a prior cursor
explicitly has `baseline_required=True`: its WAL page set is not a full database baseline,
because existing main-file pages may have no WAL frames. Missing
WAL/index state, unsupported formats/platforms, symlinks and identity-monitor failures require
reconciliation. No source checkpoint or source-write API is invoked.

Tests cover native 512-byte/64-KiB page sizes, old edits, readonly enforcement, uncommitted spill,
WAL restart with stale suffixes, corruption/torn headers, races during pinning and validation,
post-pin commits, replacement, rename-and-restore, and interruption releasing reader locks.
This component is not yet connected to sync; native changed-row/dependency mapping and bounded
adapter extraction remain required before changed-source polls can stop scanning history.

Reproduce the coordination microbenchmark with
`.venv/bin/python scripts/benchmark_wal.py --rows 100000 --coordinated`.
[Raw counters](incremental-source-view-benchmark.json). Reported logical byte counts cover the
WAL file only; WAL-index and SQLite metadata reads are additional. These measurements exclude
source setup, mutation, row mapping, sync, archive work and energy.

## SQLite row-page decoding

`adapters/pages.py` decodes rowid-table B-tree structure from private, consistent SQLite page
images. It returns page/row IDs, child edges and overflow ownership without decoding or retaining
payload values in returned metadata. Buffers are bounded to a page; traversal and overflow
metadata grow with the visited pages. The baseline walks each owned page once. A later changed
leaf can be decoded independently, while its old row locations preserve IDs for deletion checks.

The decoder handles page-one's database header, interior/leaf pages, signed 64-bit row IDs,
9-byte varints, SQLite local-payload sizing, overflow chains, and 512-byte through 64-KiB pages.
It rejects unsupported B-tree types (including WITHOUT ROWID tables), invalid pointers/key order and parent key bounds,
cell overlap, cycles/shared ownership detected during traversal, truncated data and changing
images. Images with nonempty WAL/rollback sidecars are refused; this is not a live-source reader.
Direct page consumers must check image stability before publishing their metadata; complete
walks perform the check themselves. This is a row-location decoder, not a full SQLite integrity
checker or a native cursor authority.

Native SQLite tests compare row IDs and physical leaf/overflow ownership with SQLite/dbstat,
including auto-vacuum layouts, large payloads, negative/extreme IDs, deletion/backfill and schema
B-trees. A combined test backs up the pinned SQL view while a newer commit exists, verifies the
backup still contains the older data, and relates its decoded leaf location to the next WAL
candidate. This checks the physical-page bridge between the coordinator and decoder.

The ordinary CLI collector does not yet use this component. The prepared Messages path below now combines native images, dependency routing and selective canonical capture for eligible captures.
Persistent page ownership and structural reconciliation are implemented separately below. A decoder
page set alone is not a complete changed-row set and must not advance a source checkpoint.

Reproduce the microbenchmark with
`.venv/bin/python scripts/benchmark_pages.py --rows 100000`.
[Raw counters](incremental-pages-benchmark.json) include both full snapshot preparation phases.
The selected-row phase uses already prepared images; incremental image maintenance is not yet
implemented. It reports decoder logical reads, not kernel I/O, candidate-query reads or energy.


## Persistent page ownership

`adapters/page_index.py` stores table roots, B-tree edges and key bounds, leaf row IDs,
cell descriptors and overflow ownership in a private SQLite database. It retains no payload
values. Updates require a complete changed-page set, the expected prior point, unchanged
geometry/roots and a matching source schema/policy tag. These are caller obligations; the
opaque point is not a source checkpoint or proof of capture completeness.

An update revisits changed nodes and their ancestor closure, reuses unchanged subtrees,
and returns old and new candidate row IDs. It handles splits, root collapse, removed
subtrees, truncation, overflow-only writes and page reuse between trees and overflow chains.
Key bounds and reachable ownership checks reject inconsistent graphs. Schema changes require
an explicit rebuild. Index updates roll back on failure and can join an outer transaction
through a savepoint, allowing a future private native mirror and index to commit together.
A read snapshot preserves the old graph while the writer commits a new point. Internal table
names are prefixed to coexist with native tables.

Twenty parameterized cases compare incremental metadata with a fresh rebuild, and candidate
rows with actual SQL differences, across auto-vacuum layouts, randomized changes, large
payloads, structural collapse, page reuse, schema rejection, reopen and transaction recovery.
The index is not yet used by production sync. The verified WAL provider below removes the per-update image-copy requirement for candidate
selection. The prepared native mirror below adds durable pending-row recovery. The canonical handoff below commits receipts before acknowledging prepared points.
Dependency routing and selective adapter extraction still need production integration.

Reproduce the persistent-index component benchmark with
`.venv/bin/python scripts/benchmark_pages.py --rows 100000 --persistent-index`.
[Raw counters](incremental-page-index-benchmark.json) report full image preparation separately;
index metadata and SQLite candidate-query I/O are additional to decoder logical byte counters.


## WAL-backed page images

`read_changes` retains the last committed frame descriptor for each changed page, including
its prior and current checksum. Pending tail frames do not enter that mapping. `WalPages`
retrieves bodies at those exact frame offsets and verifies page identity, salts and checksum,
then checks the named WAL identity, header and selected commit. Later appends are permitted;
reset, replacement, truncation and selected-frame corruption require reconciliation. Buffers
remain page-sized; descriptor memory grows with changed pages, not retained source payloads.
The matching `SourceReadView` must remain open to pin the SQLite source view.

`WalPageImage` combines those verified bodies with an old `PageIndex.snapshot()`. Changed
B-tree pages use the same decoder as full private images; unchanged nodes and overflow next
pointers come from the pinned metadata graph. A changed database header is checked for matching
layout and source schema version. Baseline-required views, point/schema-tag mismatches, missing
old metadata and released transactions are rejected. The caller derives the schema/policy tag
from the pinned source and must retain both snapshots through the index update.

Native SQLite tests compare every incremental graph with a fresh full-image graph across
randomized changes, structural collapse, overflow rewrites and reuse. A coordinated test makes
a second commit after pinning the source: the first update and SQL reads see the older point,
and the next continuation selects the later row. Old-edit and overflow-only tests retrieve
only one native WAL page while reconstructing the ancestor path from metadata.

Reproduce the 100,000-row component measurement on macOS with
`.venv/bin/python scripts/benchmark_wal_image.py --rows 100000`.
[Raw counters](incremental-wal-image-benchmark.json) include coordination, WAL discovery,
index updates and indexed candidate SQL reads. Only the baseline makes a full database image.
This is candidate selection, not production sync. The prepared native mirror and canonical
handoff below retain and capture selected native data. Adapter dependency routing and
production collector integration are still missing.


## Prepared native mirror

`adapters/native_mirror.py` combines the page index and tracked native tables in one private
SQLite database with FULL synchronization. Initialization makes one consistent full baseline
image for page decoding and copies the tracked tables' rows. It recreates ordinary table
schemas without installing source triggers. Foreign-key enforcement is disabled on this
projection so changed tables can be applied in any order. Unsupported virtual/generated
columns, reserved table names and inaccessible rowid aliases are refused; WITHOUT ROWID
B-trees require reconciliation. This mirror is replaceable working state, not an archive.

For continuation, the mirror requires the exact prior WAL cursor, database identity, schema
version, namespace, policy tag and acknowledged point token. Candidate rows are compared
through indexed rowid queries; unchanged values do not enter the pending change set. Comparisons
preserve SQLite storage classes, including integer versus real values. Old values are retained
for dependency/deletion routing, including BLOBs and rowid reuse. Updates stage changed values
in a file-backed SQLite temporary table before deleting/reinserting the changed set, so unique
key swaps work without holding all changed payloads in Python memory. Source reads use batches
of at most 400 candidate rows; the ownership graph and candidate IDs still scale with changed
pages/rows.

The native rows, old-value change set, page graph, candidate cursor, observation time and caller
context commit together as PREPARED. No later preparation is permitted until the exact token
is acknowledged. Reopening can read the pending rows and original observation metadata with
the source completely offline. `changes(token)` pins a read snapshot of that prepared point.
`acknowledge(token, receipt)` must only follow durable canonical capture; repeated matching
acknowledgement is idempotent and a conflicting token/receipt is rejected. The component stores
the receipt identifier but cannot itself establish that an external canonical transaction
committed. The handoff below supplies that protocol through an internal prepared-sync entry point;
ordinary CLI source collection does not use it yet.

Tests cover old edits/deletions/backfills, reused row IDs, metadata/BLOB changes, storage-class
changes, source-trigger isolation, unique swaps, competing writers and stale cursor/policy
rejection. Late transaction failure rolls back rows and page ownership together. Separate
processes exit without closing SQLite both before and after PREPARED commit. Reopening after
source deletion/WAL truncation recovers either the entire old acknowledged point or the entire
new pending point, including its original observation time.

Reproduce the 100,000-row preparation benchmark on macOS with
`.venv/bin/python scripts/benchmark_native_mirror.py --rows 100000`.
[Raw counters](incremental-native-mirror-benchmark.json) include native preparation and reading
the retained old/new values. The harness uses stand-in acknowledgement identifiers; it is not
canonical preservation proof. Adapter selection/dependency routing, durable media-byte preparation and reset/schema
reconciliation into a new mirror remain required for the production collector. Production changed-source sync still uses its existing full-source path.


## Canonical handoff for prepared captures

`prepared_sync.run_prepared_sync` binds a pending mirror token, parent/cursor, original
observation metadata and snapshot slot to a deterministic sync-run URN and SHA-256 binding.
It verifies account scope and extraction policy, including the configured Messages decoder's
file digest. A per-mirror advisory lock serializes canonical attempts across processes while
allowing adapters to create private query indexes. Extraction uses a borrowed mirror snapshot;
it never diagnoses, discovers or recopies the original native source. Cleanup retains the mirror.

Sync preserves the preparation's observation time for records and source observations. Final
completion, adapter/native checkpoints and `native_capture_receipts` commit together in the
canonical cache (schema 10). The binding is also inside the canonical sync-run snapshot metadata,
which enters the preservation journal and encrypted archives. Only after this transaction commits
does the bridge acknowledge the exact mirror token. Failed/partial extraction leaves the mirror
pending and publishes no native checkpoint or complete receipt. Already committed batches remain
preserved and can be retried under the same observation time and sync-run URN.

If acknowledgement was interrupted, a retry finds the retained complete sync-run revision and
matching receipt, then acknowledges without extraction, source access or more journal events.
It does so before checking the currently installed extraction policy, allowing completion after
a decoder upgrade. Child points require a complete canonical parent. Conflicting bindings are
rejected. A cold archive import retains the complete sync-run metadata and can rebuild the local
receipt/checkpoint projection without the original source or cache.

Integration tests exercise rollback after receipt insertion, failed/partial retries, interruption
between commit and acknowledgement, stale/bad bindings, parent/child edits and disappearance,
and two competing processes producing exactly one extraction. An encrypted export/import test
moves the source and original cache offline before reconstructing the receipt. A real Messages
adapter/Rust-decoder test captures a retained text-only mirror after moving the source directory
offline and compares all canonical heads with ordinary sync. Durable media preparation is
explicitly outside that test.

The bridge now supports the eligible selective Messages lifecycle described below. Other
captures perform ordinary adapter extraction over the mirror. The CLI does not yet select
the prepared path. WhatsApp's auxiliary databases still need a coordinated capture group.
Media files are not retained by this bridge; durable media preparation must precede production
use where file evidence is part of capture. Reset/schema reconciliation, selective dependency
routing/extraction and the full end-to-end workload/energy audit remain outstanding.

Extraction-policy encoding now excludes the schema validator runtime cache. Warming a validator
previously changed the policy hash between processes, causing unnecessary rebuilds and rejecting
otherwise identical prepared captures. The encoding version increments once to invalidate old
optimization projections; preserved records, journal entries and archives are unchanged.


## Selective Messages decoding

The Rust helper accepts `--rowids-file <json-array-file>` with at most 10,000 signed SQLite
row IDs and 256 KiB of input. It inserts a rowid predicate into the pinned library's native
message query before decoding, preserving that query's schema fallbacks, joins and correlated
context. It refuses a changed query template. Empty selection selects no messages. Missing IDs
produce no message row so callers can reconcile retained old keys. Full decoding now includes
negative row IDs; the former default `since=0` silently excluded them. The optional legacy
`--since-rowid` predicate also runs inside SQL. Invalid, repeated, unknown and conflicting
options fail before opening the database.

The JSONL header advertises `rowids-file-v1` and confirms the requested unique-ID count for a
selected invocation. `adapters/messages/decoder.py` refuses an old helper that ignores the new
option, unselected output rows, malformed/truncated JSONL, footer count mismatches or records
after the footer. Generator cleanup stops/reaps the helper and removes the private selection
file. Diagnostics go to a temporary file rather than an undrained pipe, preventing large error
output from blocking decoding. Footer statement counters describe the primary message query
and its correlated expressions, not every body/attachment query in the helper.

`ExtractStats.selected_message_rowids` exposes the internal selection mode. The Messages adapter
restricts missing-chat inference and attachment-owner lookup to selected dependencies, while
retaining global minimum-row attachment ownership. Private indexes cover reply lookup and both
directions of attachment/message joins. They persist on a native mirror; ordinary temporary
snapshots create their own. The adapter closes its SQLite connection and decoder on failure.
Selected runs do not mark message, attachment or inferred-chat tables as completely scanned,
and do not classify a deleted highest row ID as a source reset. Identity/chat/membership headers
are still read completely. Selection confirmation is excluded from message fingerprints, so
unchanged selected units match their baseline memo.

Integration tests compare selected JSONL and canonical output with full extraction, including
attachments, inferred chats, missing and negative IDs. SQLite statement counters verify a
one-row query performs zero full-scan steps with private indexes. Tests also cover malformed or
oversized selections, incompatible helpers, protocol failures and large diagnostics.

Reproduce the 100,000-message selection benchmark with
`.venv/bin/python scripts/benchmark_messages_selection.py --messages 100000` after building the
development helper. [Raw counters](incremental-messages-selection-benchmark.json) include
helper launch, decoding, Python context reads and canonical record construction. The fixture
supplies explicit row IDs and reports full snapshot/index setup separately. It does not implement
native dependency routing, deletion reconciliation or freshness for unselected canonical units.
The prepared handoff now selects eligible Messages captures as described below. The ordinary
CLI collector still uses full adapter extraction. Durable media, auxiliary-database groups,
reset recovery and production selection remain outstanding.


## Messages dependency routing

`adapters/messages/dependencies.py` routes a pending native point to message row IDs, old/new
logical unit keys, inferred-chat inputs and changed header tables. It reads before values from
the prepared change set and current values from the same pinned mirror snapshot. A token binds
the result to that point; callers must still verify that point before canonical capture. A race
test commits a newer native point while routing and confirms the result remains wholly at the
older point. `NativeMirror.changes` can now use the caller's existing read snapshot.

Direct message changes retain deleted/reused GUIDs and route old/new reply parents. Handle
identity-key changes include sender and group-action subject references. Chat GUID/presence
changes include joined messages; identifier/service changes include orphan messages whose
`ck_chat_id` can change inference. A SQLite expression index matches Python's exact
`split(';', 2)[-1]` rule. Display-name and handle-name changes only require header refresh;
they do not select message history. Membership changes likewise refresh headers.

Attachment changes route current references. Attachment-join changes include old/new message
IDs and all remaining references so ownership can transfer. Message presence changes include
shared attachment references. Chat-join changes include old/new message IDs and other orphan
supporters of the same stub-chat key. Recoverable-message joins route tombstone evidence.
All witnesses of a selected GUID are included, even when only one witness's native context
changed. Explicit media attachment IDs can enter the same dependency routing; filesystem-path
change detection and durable byte preparation are separate work.

Baseline extraction builds persistent reverse indexes. Ordinary snapshots build private indexes
as before; the router can build missing indexes once for migration. Existing native GUID indexes
are reused where SQLite confirms indexed lookup. Selected attachment-owner lookup now uses a
correlated indexed minimum rather than scanning all links of a shared attachment. A regression
with 20,000 other links bounds Python-side SQLite VM work. Query-plan tests also verify reverse
lookup and inference expression indexes. Unknown changed tables, baselines and mismatched point
metadata require reconciliation rather than guessed row selection.

Tests compare before/after full canonical message-unit outputs across body/GUID edits, deletion,
rowid reuse, replies, sender/subject identities, chat identity/inference, names, memberships,
join changes, shared ownership, stub support and recoverable tombstones. Every changed unit must
be covered by the routed old/new keys. Selection and routing implementation modules are declared
as extraction-policy dependencies, so changed routing constants/code invalidate optimization
projections rather than retaining an old policy hash.

Reproduce the combined 100,000-message component run on macOS with
`.venv/bin/python scripts/benchmark_messages_dependencies.py --messages 100000`.
[Raw counters](incremental-messages-dependencies-benchmark.json) include source-view/WAL validation,
atomic native preparation, automatic dependency routing and selected adapter extraction. They
exclude canonical cache writes, unchanged-unit freshness and targeted deletion reconciliation,
archives, durable media preparation and energy. The harness uses stand-in acknowledgements.
The prepared canonical handoff now joins those canonical steps for eligible Messages captures,
as described below and now used by eligible CLI polls. This earlier component benchmark
is not end-to-end production sync.


## Canonical summary and unit withdrawal

`Cache.coverage` now reads maintained per-source/account message counts and indexed minimum/
maximum dates. It no longer scans message history on each sync report. SQLite triggers update
counts with message-index insertion, kind/source/account moves and deletion, in the same
transaction as canonical changes. Date changes update the partial namespace/time index. Counts
include retained canonical message heads after source disappearance, matching the previous
summary semantics. Unknown dates still count as messages while contributing no date bound.

Cache schema 11 builds this projection once for existing writable caches, guarded by an atomic
version marker. Read-only older caches retain the previous summary query until a writable open
can migrate. Tests compare randomized changes, namespace moves, kind changes, purges and bounds
with the old full-scan result; they cover rollback and projection rebuild. A 10,000-message test
bounds summary SQL VM work independently of history length.

`cache/unit_coverage.py` supplies the next canonical selective-capture primitives. `UnitWithdrawal.take`
reads the affected memo's exact record/observation intervals under the canonical writer lock and
an expected source projection epoch. Missing/invalid unit coverage is refused. It also refuses
withdrawal when an unselected valid unit shares affected records, requiring expanded routing
rather than assuming exclusive ownership. An explicit join order and URN lookup keep this guard
bounded by affected memberships; a regression caught and removed a plan scanning unrelated units.

`retained` subtracts affected intervals from prior coverage without expanding historical IDs,
validating that the supplied prior capture actually covers the withdrawn unit. `reconcile`
subtracts actual newly captured/sighted observation intervals from old evidence and marks only
missing IDs absent. It preserves canonical content, records the change through normal journal
triggers and invalidates obsolete memos. It does not rely on a strictly newer wall timestamp:
deletion at the same timestamp as the prior sighting is still reconciled. The caller must supply
its tracked expected writer epoch, including only its known canonical changes.

Tests cover retained history, removed children, rebuilt evidence, shared witnesses, invalid/older
projections, same-time deletion and rollback of absence plus journal evidence. A test with
10,000 unrelated unit memberships bounds withdrawal VM work and verifies no record-body reads.
Interval tests include trillion-ID spans and randomized comparison with an enumerated oracle.

Reproduce the 100,000-message component measurement with
`.venv/bin/python scripts/benchmark_canonical_coverage.py --messages 100000`.
[Raw counters](incremental-canonical-coverage-benchmark.json) include SQLite progress-handler
instrumentation. The new summary executes fewer than 100 measured VM instructions versus
900,000 for the old query. With 100,000 unrelated memberships, one withdrawal and its journal/
invalidation/commit used about 400 measured VM instructions.

Summary queries are integrated into the current collector. The following prepared selective
lifecycle now joins unit withdrawal, durable coverage, batch epoch tracking, re-sighting,
header/stub handling and receipt commit. The primitives alone do not publish checkpoints. Complete native collection, media preparation, WhatsApp selection/auxiliary grouping
and the full end-to-end verification/energy audit remain outstanding.


## Durable selective Messages capture

`cache/capture_coverage.py` persists one active coverage attempt per source/account and its last
completed point (cache schema 12). An attempt binds the prepared token, snapshot binding and
parent to the original withdrawal intervals, retained coverage, staged output IDs and expected
source projection epoch. Canonical batches update their rows, staged IDs and expected epoch in
one writer transaction. Reopening can resume after the attempt's own partial writes without
recomputing withdrawal from an already changed memo. An unrelated semantic writer invalidates
retained coverage and requires full reconciliation from the retained native mirror.

The prepared Messages handoff now creates/resumes that attempt and uses automatic dependency
routing for eligible deltas. It withdraws completely re-read handle/chat/membership/me headers
and only affected inferred-chat keys. Selected captures stage actual canonical record and source
observation IDs; reused units contribute their revalidated intervals. The final transaction
marks withdrawn unseen observations absent, re-sights retained units, stores the new coverage
point, records the complete sync run and commits adapter/native checkpoints plus the canonical
receipt. Only then does the native mirror acknowledge its point. Failure rolls all final steps
back together; previously captured batches and their original withdrawal remain durable.

A separate-process test exits without closing SQLite after a selected batch removes a cloud-only
attachment but before completion. The retry uses the original old attachment observation even
though the partial batch has already replaced its unit memo. Final absence is recorded without
losing the old attachment record. Other tests cover receipt interruption, unchanged-unit and
stub freshness, old edits/deletion, header changes, scoped epoch conflicts and explicit full
reconciliation. A full coverage rebuild can still reuse individually validated extraction memos;
only an explicit full request disables memo reuse.

Eligibility is currently conservative: the helper must confirm row-ID selection, affected message
GUIDs must be ASCII UUIDs and the native schema must enforce unique GUIDs. File-backed attachment
paths force full extraction because SQL-only evidence cannot authorize fresh file observations. Cloud-only rows without paths can
use the selective path. Unsupported routing, missing/invalid coverage or unrelated canonical
writes also reconcile the full retained mirror. These fallbacks preserve correctness but do not
satisfy the complete issue's bounded-work requirement for all supported workloads.

Eligible CLI Messages polls now use this handoff. Bounded source WAL expiry/reset discovery,
automatic policy/schema rebase dispatch, durable file-byte evidence, incremental header
materialization, WhatsApp routing and coordinated auxiliary databases still need work. Raw native
projection modules are now included in the prepared policy tag; the complete implementation and
external dependency closure still needs a final policy-invalidation audit. Native
poll cadence/retention are unchanged and the full issue remains incomplete.

Reproduce the 100,000-message text-only canonical run on macOS with
`.venv/bin/python scripts/benchmark_prepared_messages.py --messages 100000`.
[Raw counters](incremental-prepared-messages-benchmark.json) include coordinated WAL validation,
native preparation, automatic selection, canonical cache writes, coverage/freshness/absence,
receipt/checkpoint commit and native acknowledgement. Unlike the earlier component harness,
this run uses actual canonical receipts. It excludes archive/encryption/import/delivery,
file-backed media, WhatsApp, kernel I/O and energy, and is not the final full-workload audit.


Large Messages selections now stream through helper children of at most 10,000 IDs each.
The adapter orders selected IDs by native date and row ID, reads metadata once, and combines
only verified children into one final footer. Full and selected helper queries use the same
deterministic date/row/chat/deletion ordering. Missing selected rows still count toward the
requested selection and produce no message, allowing targeted absence reconciliation.
A later child failure or policy-header drift leaves the capture incomplete. The native
10,000-ID per-process input bound remains in place; routed capture size no longer forces
full extraction. An integration test compares 10,001 selected messages against full output
with reversed native dates across the boundary. Protocol tests cover multiple children,
aggregate counts, header drift, interruption and mismatched declared selection counts.
This removes one fallback from the prepared Messages path; complete production dispatch,
file-byte preservation, WhatsApp routing, bounded source reset discovery and the complete workload and
energy audit remain outstanding. No energy or large-batch speedup claim is made here.


Prepared policy tags now include the loaded WAL reader, pinned source view, page decoder,
page index, WAL page image, native mirror and vnode monitor implementations. Extraction
signature format 6 additionally includes function defaults, keyword defaults, property
accessors and each declared decorator's wrapped function body. It continues to hash loaded
code rather than replacement files on disk, and excludes the schema validator runtime cache.
Tests change each of the seven raw projection layers after prepare: canonical capture
refuses the policy mismatch, leaves the pending mirror point intact, and succeeds after
the implementation is restored. Completed receipt recovery remains before policy checking,
so an already committed capture can still finish acknowledgement after an upgrade.
Policy mismatch requires a safe rebase before production dispatch; the explicit operation below
now supports acknowledged points, while automatic dispatch and pending-upgrade recovery remain
outstanding.


`NativeMirror.rebase` now rebuilds an acknowledged projection in one transaction, replacing
its registered native tables and page graph with a pinned full source view. The new prepared
baseline retains the exact prior token as its canonical parent. It can change schema, policy
and registered table sets; it cannot replace a pending capture, accept a stale token or change
account namespace. A failed or abruptly terminated transaction restores the old table schemas,
rows, page graph and acknowledgement receipt. An abrupt exit after commit leaves the entire
new prepared baseline recoverable even after the source has expired.
Real Messages integration tests exercise schema change, policy change and WAL-reset rebases,
then verify old-message absence without losing canonical content, edited-message revision
history and subsequent one-message selective capture. A rebase is a full reconciliation,
not the bounded-work solution for routine WAL expiration; efficient reset discovery, automatic
production dispatch and recovery of a pending capture across an implementation upgrade still
need work. Existing archive captures and ciphertext are never modified by mirror rebasing.


## CLI native Messages dispatch

`chatstore sync --source messages` now uses the durable native projection for eligible
WAL-backed Messages sources, across both independent invocations and `--watch-interval`
polls. Mirrors live under the private data directory's `native/` folder, keyed by source
and account scope. The same advisory lock covers mirror opening, preparation, canonical
capture and acknowledgement, serializing competing collectors before SQLite schema setup.
Pending captures are resumed before source discovery, including when the native database
is unavailable. No fallback is allowed after a new point has been prepared. A successful
continuation prepares changed rows; acknowledged policy/schema/reset changes can prepare a
full rebase. Unsupported fresh source views use the existing ordinary snapshot path.
Only the adapter's required tables and the optional recoverable-message join are registered;
unrelated native tables do not trigger unknown-table reconciliation. Dispatch implementation
is included in prepared policy signatures.

CLI tests verify separate-invocation old edits, clean polls, deletion, schema rebase and
canonical failure followed by retry with the native database offline. Sources with any
file-backed attachment path still use ordinary capture. Full baselines may scan source
attachment history for eligibility; delta polls instead validate the indexed private
candidate before commit, as described below. Complete header materialization is still
repeated and remains a bounded-work gap. Native polling currently opens a source
coordinator per call, rather than retaining it for the collector lifetime. WhatsApp remains
on its existing extraction/monitored-reuse path. Neither complete production efficiency
nor the full preservation/energy acceptance audit is claimed by this dispatch milestone.


## Bounded attachment eligibility on native deltas

Native mirror preparation now accepts a pre-commit row validator, run after candidate row
and page-graph updates but before publishing the prepared point. Ordinary Messages delta
dispatch uses it to check the candidate attachment filename index. A newly introduced file
path rejects the candidate transaction and routes to ordinary media capture, without
publishing or acknowledging a new native token. The old mirror rows, page point and
acknowledgement remain intact. Once file paths are removed, a later delta can cover all
changes since that old point; intervening canonical changes trigger the existing full
coverage reconciliation when needed.

Full initial/rebase captures still perform the source eligibility query. Continuations
no longer query source attachment history to establish eligibility. Tests trace the source
SQL with 2,000 cloud-only rows, require no attachment-table query on a clean continuation,
and bound the private lookup to fewer than 30 SQLite VM instructions with 100,000 unrelated
cloud-only attachments. CLI tests introduce a linked file-backed attachment after a text-only
baseline, verify rollback plus ordinary capture, then remove its path and resume native
capture. A separate mirror test injects validator failure and verifies old rows, page cursor,
changes journal and retry behavior. This removes the repeated eligibility scan for eligible
deltas; durable media-byte preparation and bounded collection of media-bearing sources remain
outstanding. No end-to-end speed or energy claim is inferred from the VM bound.


## Durable media-byte retention primitive

`adapters.media_capture.capture_attachment` now copies and hashes a regular source file
in one pass with 1 MiB buffers, flushes and fsyncs its private temporary file, and publishes
it into the existing content-addressed blob store using a non-replacing hard link. The
source is opened without following a final symlink and without blocking on FIFOs. Its
inode/device, size, nanosecond modification/change times and final path identity must still
match after copying. Mutation, replacement or disappearance fails capture before available
evidence is returned. Missing, purged, cloud-only and unsupported file observations preserve
the existing Messages probe semantics.

A successful result contains a serializable availability/hash/size/path-hint observation.
Its bytes survive source disappearance and abrupt collector exit. Existing retained blobs
are checked against their address and never overwritten; corrupt content fails loudly.
Directory fsync also covers retry after an earlier failed bucket creation/publication.
Failed attempts can leave an unreferenced immutable blob but cannot return successful
evidence. Temporary copies are removed on ordinary failure. Tests cover multi-buffer and
empty files, one-pass source reads, non-regular files, source mutation/replacement/deletion,
existing-blob corruption, fsync failure/retry, deduplication and subprocess abrupt exit.

This primitive is not yet enabled in the collector. The next integration must bind media
row IDs, native filenames and these retained observations to the same durable prepared
point, then make adapter extraction consume that evidence without reopening the live file.
File-only changes also need bounded watcher routing and unchanged-file evidence before
media-bearing captures can use retained coverage. Calling this copier on every historical
file every poll would add work and is not the intended integration. Encrypted archive
inclusion, source expiry/cold restore and energy/workload tests must exercise the completed
path before the issue can be considered satisfied.


## Point-bound prepared media evidence

The native baseline and rebase operations now accept the same pre-commit preparation hook
as deltas. `adapters.prepared_media.prepare_media` uses that hook to retain attachment bytes
and write per-row media observations plus the candidate page token in the mirror's SQLite
transaction. Full baselines enumerate attachments; deltas visit changed native attachment
rows plus an explicitly supplied complete file-change set. Omitting that set rejects the
delta. Removed native attachments remove their current media projection without removing
retained blob bytes or canonical history. A rejected transaction restores the previous media
observations and point along with native rows/page ownership; already retained content-addressed
bytes may remain safely unreferenced.

A prepared capture can declare `messages_media: {format: 1}` in its bound context. Its borrowed
snapshot then supplies a retained probe to Messages extraction. A single SQLite statement
checks the media point, native capture token and attachment row together, then verifies the
native filename. Missing/mismatched evidence fails capture; extraction never falls back to
opening that attachment's live source path. The retained probe participates in the existing
unit fingerprint, so a file-only change creates a new attachment revision and keeps older
bytes. Media preparation and capture implementation modules are included in extraction policy
signatures.

Integration tests capture an attachment after deleting its source file and taking the native
database offline, export available media into an encrypted incremental archive, take the original
blob store offline, and restore the bytes from that archive alone. Other tests retain both
file-only versions, remove current evidence after native deletion, reject stale/missing evidence,
roll back after media copying, require explicit file-change coverage, and recover a media point
after a subprocess exits immediately following its commit.

Messages collection with `sync --watch-interval` now retains a `PreparedMediaWatch` in its
sync session. It registers attachment paths before copying, maps shared paths to every owner,
and commits the complete dirty attachment-ID set with the candidate media point. Canonical
selection routes those IDs to their owning messages even when no source SQLite page changed.
Clean continuations reuse frozen media evidence without opening attachment files. Initial
capture, process restart, rebase, or a rejected publication requires full media reconciliation.
Standalone collection without a retained watcher still uses ordinary capture for file-backed
sources. The final production efficiency/energy requirements remain unverified.


## Routed live file-change notifications

`SourceMonitor.dirty_paths()` now returns the registered paths affected by vnode events
and SQLite `data_version` changes. Dirty paths remain pending across polls and failed
capture attempts; only a stable successful `accept_capture` clears them. New registration
always invalidates an earlier token, including a second missing sibling that reuses all
existing ancestor watches. Clean dirty-path polls perform no per-file stat, open or path
enumeration.

Event routing uses a descriptor-to-target map plus per-target/per-event dependency sets.
A parent directory write needed for a missing child does not visit or dirty unrelated
existing siblings. Once the child is created, rearming removes an unnecessary parent-write
subscription. Namespace events can still affect several missing children of one directory;
all such dependencies are conservatively dirty because vnode events do not name the child.
Ancestor permission/identity events similarly invalidate their actual dependent paths.

Replaced, removed and newly created paths must be rearmed before capture acceptance.
`dirty_paths` performs that work before returning its set. Manual rearming also records
identity changes before closing the old descriptor, so cancelling a queued old-inode event
cannot lose the dirty path. Subsequent writes through a handle on the replacement remain
observable. `unwatch` retires removed media and releases now-unused file/ancestor handles;
shared dependencies remain registered. Monitoring errors and descriptor exhaustion continue
to fail closed. Database handles require a new session rather than media-style retirement.

Tests cover sparse changes among 100 files, repeated polling and failed acceptance, open-handle
writes after replacement, missing-child creation without existing-sibling invalidation,
new missing-sibling registration, clean stat/open-free polling, database commits, retirement
and manual rearm before event polling. Existing monitored-sync/source-view tests also pass.
`MediaWatch` maps paths to attachment IDs and retires watches after their last owner disappears.
`PreparedMediaWatch` binds that mapping to the private mirror path and exact parent token.
Mutation during retention rejects the native transaction; a rolled-back candidate cannot
authorize reuse on the next attempt. Session shutdown closes all media descriptors. Tests
exercise the production coordinator with clean, file-only edit and deletion polls, preserving
the prior attachment revisions. Descriptor exhaustion and incomplete symlink coverage fail
closed; large attachment populations still need the final resource audit.

The watch loop reports media retention or monitoring failures with the JSON error code
`capture_retry_required`, then waits for the normal poll interval and attempts capture again.
It does not fall back to unmonitored media reuse. The rejected preparation rolls back its
SQLite evidence and point; already retained content-addressed bytes may remain unreferenced.
Tests cover recovery on the next poll and repeated failures, including closed monitor handles
and no native/canonical checkpoint advance. A finite `--max-polls` run returns the final poll's
exit code, so an unresolved final capture failure remains a failure. Other exceptions retain
their existing CLI behavior. [Resource measurements](incremental-benchmark.md#live-media-watcher-resource-limits)
show successful sparse routing at 10,000 files but failed registration at 100,000 files.
One descriptor per file and linear watch memory remain an acceptance gap; no clean coverage
is accepted after resource exhaustion, and failed attempts close their monitor handles.


The routed monitor also exposed a collector boundary condition: an unrelated directory
write could leave a missing optional database awaiting rearm when a full capture began.
`SyncSession.begin` now rearms dirty dependencies before its pre-capture token and registers
SQLite version coverage for optional databases that appeared since initial preparation.
This both restores immediate clean reuse after an old source edit and prevents a newly
created auxiliary database from being covered only by a main-file vnode while its later
commits stay in WAL. A dedicated integration test creates that database between session
preparation and capture and confirms that a subsequent WAL commit invalidates coverage.

## WhatsApp ChatStorage dependency routing

`adapters.whatsapp.dependencies.route_changes` classifies the exact pending native mirror point.
It routes changed messages to complete `(ZCHATSESSION, ZSTANZAID)` groups, retains old unit keys
for deletions and key changes, and includes replies whose parent identity may have changed.
Media and data-item reparenting selects both old and new message owners. Chat JID changes select
that chat's message groups; title-only changes require header work without selecting unrelated
messages. File discovery can supply explicit message IDs separately. Private indexes support
stanza, parent and dependent lookups; no source indexes or triggers are installed.

Tests exercise WAL-backed edits, deletion, duplicate stanza rows, key changes, replies, media
reparenting, data-only changes, chat identity/title changes and file-only selection. Baselines,
a mismatched pending token, and unclassified table changes reject selective reuse. This is a
routing primitive; atomic auxiliary-database capture and collector handoff still require
integration. It does not authorize a SQL-only point to skip media checks, or prove end-to-end
proportional work for WhatsApp.

The WhatsApp adapter now accepts `ExtractStats.selected_message_rowids` for message extraction.
A private temporary selection drives indexed stanza lookups, including unselected duplicate
rows in each affected stanza. Selected groups produce the same canonical revisions as full
extraction. An empty selection or a deleted ID emits no unrelated messages; selected extraction
does not declare the message/media tables fully scanned or infer a source reset from a deleted
high ID. Explicit full adapter reconciliation continues to visit all groups.

Tests compare selected duplicate-group output with full output and count SQLite VM work while
unselected history grows from 100 to 10,000 rows. That bound covers message grouping and its
dependencies only. The collector does not yet dispatch WhatsApp through prepared selective coverage. Header work, joint auxiliary
capture, frozen media evidence, canonical withdrawal/coverage and collector integration remain
necessary before the WhatsApp end-to-end efficiency claim can be made.

WhatsApp header JID discovery now uses private indexes and successive greater-than seeks for
sender, recipient and incoming-recipient values. Unlike stepping a `SELECT DISTINCT` index
scan, each lookup jumps past all repeated occurrences of the preceding value. The incoming
recipient index is partial on `ZISFROMME=0`, preserving the owner-identity rule and alias closure.
Tests compare the complete results with `DISTINCT` across null/empty/Unicode values, old edits
and deletions. Warm SQLite VM work remains bounded with fixed distinct keys while history grows
from 100 to 100,000 messages. Building those indexes still requires a baseline pass. Ordinary
snapshot collection rebuilds private indexes; only a retained native mirror can amortize that
cost. Header emission still scales with distinct identities and other header rows, so this
change does not establish fully incremental header materialization.
