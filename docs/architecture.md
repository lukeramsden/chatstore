# Architecture

```text
source databases (read-only)
    │
    ├── WhatsApp adapter (Python, SQL over ChatStorage.sqlite snapshot)
    └── Messages adapter (Python SQL + Rust helper for attributedBody decoding)
                                          │
                                          ▼
                              canonical record stream
                     (versioned JSONL records, specs/canonical-model.md)
                                          │
                          ┌───────────────┴────────────────┐
                          ▼                                ▼
                SQLite + FTS5 cache               encrypted archives
            (rebuildable, user-local)       (catalogue + one ZIP per UTC month)
                          │                                │
                  CLI / JSON / agent skill          verify / import / restore
```

The canonical records, the URN derivation and the archive format are the durable contracts
([specs/](../specs/README.md)). SQLite tables, indexes and FTS are implementation details that can
be rebuilt from either a live source or a set of archives.

## Module map

`src/chatstore/` (≈6,100 lines of Python) plus `helpers/messages-decoder/` (Rust).

| Module | Responsibility |
| --- | --- |
| `paths.py` | `DataDir` layout, `resolve_data_dir` (`--data-dir`, `CHATSTORE_DATA_DIR`, platform default) |
| `config.py` | `Config` (`config.json`) and `Identity` (`identity.json`: scopes, scope mappings, export set) |
| `identity/urn.py` | Frozen UUIDv5 derivation, canonical tuple serialisation, minted IDs |
| `canonical/` | Record constructors, JSON Schemas (generated to `specs/schemas/`), schema version |
| `adapters/base.py` | `SourceAdapter` protocol, `Diagnosis`, `ExtractStats` (incl. `scanned_tables`, `full_scan`) |
| `adapters/snapshot.py` | Read-only open + SQLite online-backup snapshot into `<data-dir>/snapshots/` |
| `adapters/whatsapp/` | Discovery, schema check, extraction, duplicate-row collapse, JID/LID aliases |
| `adapters/messages/` | Discovery, Full Disk Access diagnosis, helper invocation, orphan-row chat inference |
| `cache/schema.py`, `migrations.py` | Tables below; versioned migrations |
| `cache/store.py` | `Cache`: `upsert_many`, `observe_many`, revisions, heads, bulk mode, `set_head`, `merge_observations` |
| `cache/fts.py`, `queries.py` | FTS5 maintenance; `search`, `read_chat`, `context`, `resolve`, media views |
| `sync.py` | `run_sync`: snapshot → extract → upsert → reconcile → `sync_runs` row |
| `archive/manifest.py` | Manifest construction (versions, scopes, bounds, lineage, counts, digests, media policy) |
| `archive/container.py` | WinZip-AES-256 ZIP with a single `payload` TAR member; safe extraction limits |
| `archive/export.py` | Bucket planning, context selection, revision numbering, atomic publish |
| `archive/verify.py` | `load_archive` – the single validation boundary producing a `LoadedArchive` |
| `archive/import_.py` | Lineage decisions, blob restore, transactional apply, ledgers, scope adoption |
| `archive/password.py` | Keychain (`security -i`), `CHATSTORE_PASSWORD_FILE` (0600), tty prompt |
| `curation.py` | People, identity links, suggestions, scope mapping, conflict resolution, purge |
| `media.py` | `local_state` derivation: restored blob / source file / absent |
| `helper_install.py` | Download + sha256-verify the release helper into `<data-dir>/bin` |
| `cli/common.py` | Typed CLI contract: exit codes, `Ctx`, `Result`, `CliError`, paging, freshness |
| `cli/app.py`, `cli/extra.py` | Argument parsing and command implementations; envelope emission |

The Rust helper `chatstore-messages-decoder` wraps the pinned `imessage-database` crate and emits
framed JSONL (`chatstore-messages-decoder/1`) for typedstream bodies, edit history, tapbacks and
attachment rows. Everything else about Messages is plain SQL in Python.

## Storage layout

```text
~/Library/Application Support/chatstore/      (or --data-dir / CHATSTORE_DATA_DIR)
  config.json        timezone, source path overrides, limits, helper_path
  identity.json      account scopes per source, scope mappings, export-set URN
  cache.sqlite3      records, revisions, indexes, FTS, ledgers (WAL mode)
  blobs/sha256/      restored attachment bytes, content-addressed
  bin/               helper binary installed by `chatstore helper install`
  snapshots/         temporary source snapshots, removed after each sync
  exports/           default archive destination
  staging/           decrypt/validate area for import; cleared on completion
```

Directories are created 0700 and files 0600. The cache and blobs are plaintext protected only by
OS access control and optional full-disk encryption; they are not application-encrypted.

## Cache schema (implementation detail)

| Table | Contents |
| --- | --- |
| `records` | Current head per URN: kind, source, scope, JSON body, `revision_digest`, `observed_at` |
| `revisions` | Every observed content version: `(entity_urn, revision_digest)` unique, `supersedes`, `observed_at` |
| `messages_idx`, `events_idx`, `attachments_idx`, `parts_idx`, `memberships_idx`, `links_idx`, `aliases_idx` | Typed projections for filtering and joins |
| `source_observations` | Native keys, fingerprints, adapter versions, `present` ∈ {present, absent_from_source} |
| `checkpoints` | Per-source incremental cursors |
| `conflicts` | Content and archive-branch conflicts awaiting `conflicts resolve` |
| `imports`, `exports`, `bucket_membership` | Archive ledgers and which URNs each bucket revision owns |
| `meta` | Schema version, identity namespace check |
| FTS5 virtual tables | Keyed by `messages_idx.id` and `revisions.id`; fully rebuildable (`rebuild-index`) |

Two rules make the revision model coherent:

- **A `revision_digest` identifies content, not an occurrence.** It is SHA-256 over the canonical
  record minus observation metadata. Re-observing content that was seen before does not create a
  new row; it updates that row's `observed_at`/`supersedes`.
- **The head of a URN is the most recently observed content.** A live sync always wins for its own
  source. Archive import decides within the archive's own lineage by observation time; content the
  cache holds that the archive's lineage has never seen is a conflict, never a clock decision.

## Sync

`chatstore sync` runs per source:

1. `diagnose` – path found, readable, schema compatible, helper present. Incompatible schema fails
   closed (exit 4) rather than emitting misleading records.
2. Snapshot the database(s) with the SQLite backup API into `snapshots/` (captures committed WAL
   data; the source is opened `mode=ro`; lock waits are bounded).
3. Extract canonical records in batches of 5,000 within `Cache.bulk()` (WAL autocheckpoint off,
   `synchronous=NORMAL`, checkpoint+truncate on exit).
4. `upsert_many` compares fingerprints: `unchanged`, `updated` (new revision), `created`.
   `observe_many` records native keys.
5. Reconcile: for `initial`, `--mode full`, or an adapter-detected source reset (max row id went
   backwards), every observation in `seen_tables ∪ stats.scanned_tables` not seen this run is
   marked `absent_from_source`. Records stay current and searchable; only `purge` deletes.
6. Write a `sync_runs` row with mode, counts, coverage, unsupported kinds, errors and status.
   Any per-record error makes the run `partial` (exit 5). A partial sync is never called complete.

Incremental strategy differs per source: WhatsApp uses a `Z_PK` checkpoint plus a 5,000-row
look-back for status changes; Messages re-decodes every row each run and relies on fingerprint
comparison because edits, receipts and tapbacks mutate old rows (~10 s for 77k rows).

## Archives

**Export** (`archive export`): plan buckets = UTC months present in `messages_idx` ∪ months
previously exported (so an emptied month writes an empty superseding revision); for each bucket
collect messages, parts, events, attachments and revisions by original message time, plus context
(`full`: the chats/identities/memberships/aliases needed; `minimal`: stubs). Compute the content
digest over sorted `(urn, revision_digest)` pairs (context excluded) plus media policy; skip if
unchanged since the last revision unless `--force`. Write `manifest.json`, `records/*.jsonl`,
optional `blobs/sha256/*`, `checksums.json` into a TAR, then into a single AES-256 `payload` member
of a ZIP in staging, and atomically publish `chatstore-<set>-<label>-r000N.zip`. The catalogue
archive carries scopes, identity mappings, people, links and every entity with no bucketed messages.

**Verify / import**: `container.py` authenticates and extracts under limits (64 MiB per member,
1 GiB total, ratio 200, 512 MiB per blob; no absolute/traversal paths, symlinks, duplicates,
extra ZIP members or ZipCrypto). `verify.load_archive` then checks manifest shape and lineage,
every member checksum against `payload_digest`, every record's schema and recomputed
`revision_digest`, revision/observation/stub rows, all manifest counts and blob naming, producing a
`LoadedArchive`. Import consumes only that object:

1. Lineage: `proceed`, `already_imported`, `superseded`, `older`, `updated` or `branch_conflict`
   (exit 6, recorded; resolve with `conflicts resolve --accept`).
2. Restore blobs first (content-addressed, harmless if the transaction later fails); a failure is
   `media_failed` (exit 1) and nothing is recorded, so a re-run retries.
3. One transaction: apply records/revisions/observations, retire records owned by the superseded
   bucket revision that the new revision no longer contains, record `bucket_membership` and the
   `imports` ledger row.
4. Adopt scopes and export set into `identity.json`; rebuild FTS for touched rows.
5. Report coverage, `unresolved_refs`, `media_not_restored`, `blobs_restored`, conflicts.

Imports are idempotent; `already_imported` still restores missing blobs and re-adopts scopes.

## CLI contract

All commands accept `--json` and emit the `chatstore-cli-v1` envelope
(`{version, command, ok, data, warnings, freshness, page}`); read commands page with
`--limit/--cursor` and truncate at `max_chars`. Exit codes: 0 ok, 1 failure, 2 usage,
3 permission, 4 incompatible, 5 partial, 6 conflict, 7 invalid archive, 8 not found,
9 not initialised. Search and read never sync, export or touch the network. Passwords never appear
in argv, environment values, logs or JSON.
