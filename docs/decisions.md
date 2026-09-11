# Decisions

Every non-obvious choice, what was decided, and the evidence behind it. Grouped by topic;
[history.md](history.md) gives the chronology. Measurements are from one real machine
(≈196k WhatsApp and 77k Messages logical messages); only counts are recorded, never content.

## Packaging and distribution

- **Name and form**: `chatstore`, a Python package exposing a `chatstore` console script, MIT.
  Published on GitHub (`lukeramsden/chatstore`); installable with `pip`/`pipx` from a Git URL.
  Version `0.1.0`; tags `v*` trigger the release workflow.
- **Rust helper distribution**: built from source with a pinned toolchain (`rust-toolchain.toml`,
  1.95) for development; prebuilt macOS arm64/x86_64 binaries with `SHA256SUMS` are attached to
  each release and installed by `chatstore helper install`, which verifies the digest before
  writing. The download is optional; a local build is equivalent.
- **CI**: `ci.yml` runs ruff, mypy and pytest (with the helper build) on macOS for every push.
  Release builds use `macos-14` (arm64) and `macos-15-intel` (`macos-13` runners are retired).
- **Build note**: on the development machine `xcodebuild` in Xcode 26.2 aborts, so `cargo` needs
  `DEVELOPER_DIR=/Library/Developer/CommandLineTools`; `scripts/build-helper.sh` detects and applies
  this. Not a project requirement.

## Supported sources and parser pins

- **WhatsApp**: the Core Data schema recorded in `specs/source-findings.md`. `doctor` checks the
  required tables/columns and fails closed (exit 4) on mismatch; no promise is made about other
  WhatsApp versions or platforms.
- **Messages**: `imessage-database = "=4.2.0"` (released 2026-06-18), requiring Rust ≥ 1.85
  (edition 2024 via rusqlite 0.40). Upstream feature claims were treated as claims until the crate
  was verified against real schemas and synthetic fixtures. Apple binary-body decoding is not
  reimplemented.
- **Rust helper scope** is deliberately narrow: typedstream bodies, edit history, tapback/variant
  classification, attachment rows. Handles, chats, joins and `ck_chat_id` inference stay in Python
  SQL so most logic is in one language. Output is versioned and framed; the adapter fails closed on
  a format or exit-code mismatch. Decoding 77k rows takes ~1 s.

## Identity

- **Namespace** frozen at `3f14f01c-5cc4-4325-950f-a56812da489b`; tuple
  `["chatstore-id-v1", source, account_scope, entity_type, native_key]`; test vectors published in
  `specs/test-vectors/` — changing them is a breaking change.
- **WhatsApp message key** `(chat JID, sender, stanza ID)`, validated: zero null stanza IDs, and
  duplicate rows (history sync, `ZSORT < 0`) never differ in date/sender/direction. Duplicates
  collapse into one message; all row ids are kept in `source_observations`.
- **Messages handle identity** is `(service, id)`; 184 handle ids appeared on more than one
  service. Cross-service unification is a suggested identity link, never automatic.
- **People and links are source-independent** (`source: null`). Link URNs are UUIDv5 over
  `(person_urn, identity_urn)` so re-linking is idempotent; unlink sets `state=rejected` and keeps
  the record as evidence. Suggestions match the last 10 digits of a phone number or an exact
  lower-cased e-mail across *different* sources; listed, never auto-applied. No merging by display
  name.
- **Scope mapping never rewrites records.** `scope map <from> <to>` emits `aliases`
  (`reason: scope_map`) for every non-minted record in `<from>` by re-deriving the URN in `<to>`,
  and stores the mapping in `identity.json` and the catalogue. Independently initialised imports do
  not deduplicate until mapped; there is no implicit mapping.
- **Minted IDs**: when no trustworthy native key exists (e.g. Messages orphan stub chats), a
  persistent UUID is minted and the mapping plus matching evidence retained.

## Canonical model

- **Timestamps** in JSON are `{raw, unit, epoch, utc_ms}`; `raw` is a decimal string because
  Messages nanosecond values exceed 2^53.
- **Messages orphan rows** (no `chat_message_join`; 79% of rows on the test machine) are assigned
  via `ck_chat_id` with `chat_assignment = inferred_ck_chat_id`; a stub chat keyed by the
  `ck_chat_id` string is created when no `chat` row matches.
- **Purged attachment placeholders**: inline references whose `attachment` row is gone (1,819 of
  2,297) become `attachments` with `availability = missing`, keyed `["purged", "<ref>"]`.
- **Shared attachment rows** (one row joined to several messages; observed once) are owned by the
  lowest message ROWID; other messages reference its URN.
- **Unknown WhatsApp codes** are preserved verbatim as `unknown:<n>` and counted under
  `unsupported`. Observed but unmapped on the test machine: message types 10, 12, 13, 14, 19, 20,
  23, 27, 28, 30, 32, 41, 42, 43, 46, 54, 59, 60, 63, 66, 73, 75, 76 and many `ZGROUPEVENTTYPE`
  values.
- **Attachment records are exported verbatim** even in text-only archives. Rewriting
  `availability` to `not_exported` would break the revision digest; the manifest counts
  `media.not_exported` and import reports `media_not_restored` instead.
- **`availability` vs `local_state`**: `availability` is the source observation as last synced
  (stored, archived). `local_state` (`restored` / `source_file` / `absent`) is derived from the
  filesystem at query time and never stored. Before this split a text-only restore reported
  attachments as present on the machine.

## Revisions and heads

- **A revision digest identifies content, not an occurrence.** SHA-256 over the record minus
  observation metadata; `(entity_urn, revision_digest)` is unique. Re-observing earlier content
  updates that row's `observed_at`/`supersedes` rather than being ignored.
- **The head is the most recently observed content.** Live sync always wins for its source. Import
  decides within the archive's own lineage by observation time (`updated` / `older`); content the
  lineage has never seen is a conflict. Ancestry-chain walking was removed after an A → B → A edit
  left B current on restore (the second A had been dropped as a duplicate row and B then looked
  like a descendant).
- **Retention**: all observed revisions are kept by default; purge is explicit. Observation
  history is never presented as complete source edit history.
- **Conflict resolution** picks a revision digest (`--keep`); the chosen revision becomes head and
  supersedes the previous one. Archive branch conflicts are resolved with `--accept`, after which
  the branch imports normally.

## Cache and sync

- **FTS tables are keyed by rowid** (`messages_idx.id`, `revisions.id`); deleting by an UNINDEXED
  column was a full-table scan and made sync superlinear.
- **Performance**: `synchronous=NORMAL` under WAL, batched `upsert_many`/`observe_many`, and
  `Cache.bulk()` (no WAL autocheckpoint during a run, checkpoint+truncate on exit) took a full
  WhatsApp sync from 282 s to 119 s. A crash loses at most the last commit; sync re-runs.
- **WhatsApp incremental**: `Z_PK` checkpoint plus 5,000-row look-back for status changes.
  Checkpoint going backwards = source reset → automatic `full_reconcile` in the same run.
- **Messages incremental** re-decodes every row and relies on fingerprints (~10 s) because edits,
  receipts and tapbacks mutate old rows. A row-id checkpoint would miss them.
- **Source absence is evidence, not deletion.** Full reconcile marks
  `source_observations.present = absent_from_source`; records stay current and searchable.
- **Adapters report `scanned_tables`**, because reconciling only tables that emitted a row meant a
  completely emptied table was never reconciled.
- **Partial vs complete**: any per-record extraction error marks the run `partial` (exit 5). Honest
  over tidy. One WhatsApp message without a chat session does this on the test machine.
- **Purge** deletes records, revisions, observations, index rows and FTS entries for an entity
  (messages take parts/attachments/events; chats take messages and memberships) or a whole
  source/scope, always warning that exported archives are unchanged.

## Archive format

- **Encryption library**: `pyzipper 0.4.0` (2026-05-14, on pycryptodomex). Evaluated: 8 MB
  round-trip with 64 KiB streaming reads OK; wrong password → `RuntimeError("Bad password")` before
  any plaintext (password-verifier bytes); ciphertext byte flip → `BadZipFile("Bad HMAC check")`
  **only at end of stream**, so import decrypts fully into staging and treats the payload as
  untrusted until the stream closes clean. Interop: `7zz 25.x` reads pyzipper output and vice versa;
  macOS `unzip 6.00` cannot read AES ZIPs (documented).
- **Container**: one neutrally named encrypted `payload` member (a TAR) so internal filenames do
  not leak; ZIP compression only, no extra layers; deterministic member order before encryption.
  WinZip AES's KDF is weak by modern standards, hence generated 256-bit passwords and no claim of
  suitability for human-chosen passwords. A stronger scheme would be a separately versioned
  envelope, not a custom cryptosystem.
- **Manifest binding**: `payload_digest` is a digest over `checksums.json` entries rather than of
  the TAR (which contains the manifest). Every member checksum is verified before any record is
  read.
- **Filenames reveal month and revision** (`chatstore-<set>-<YYYY-MM>-r000N.zip`); the prefix is the
  export-*set* ID, not the account scope, so the source is not revealed.
- **Attachments excluded by default** (`--media text`); `available-media` is opt-in, hashes and
  verifies bytes, never fetches remote media, and deduplicates blobs per archive (duplication
  between buckets accepted over shared blob packs, which would add restore dependencies).
- **Bucket context duplicated by default** so each monthly bucket is interpretable alone; with
  ~9.7k LID aliases and ~19k identities recurring, 81 text buckets total ~290 MB. `--context
  minimal` (stubs, catalogue required) is the smaller alternative. Bucket content digests exclude
  context records, so a chat rename no longer re-exports every month.
- **Revision numbering** is per `(export_set, kind, label)`. Unchanged content writes nothing;
  `--force` writes anyway.
- **Empty buckets supersede**: previously exported buckets stay in the plan; an emptied one writes
  an empty `r000N+1` so restores retire its records. Never-exported empty buckets are skipped.
- **Import lineage**: an archive covered by a later imported revision is `superseded`; a same-bucket
  archive whose lineage lacks the imported one is `branch_conflict` (exit 6, recorded). Winners are
  never chosen by filename, mtime or local revision number alone.
- **One validation boundary** (`archive/verify.load_archive`) recomputes every record's digest,
  validates revisions/observations/stubs and manifest shape/lineage/counts and blob naming, and
  hands import a `LoadedArchive`. Container authentication proves bytes are intact; it never proved
  the producer built valid records (14 stale dev archives from before the verbatim-attachment fix
  were caught this way).
- **Import completion covers records, `identity.json` and blobs.** Blobs restore before the ledger
  commit; failure is `media_failed`, unrecorded, so a re-run retries; `already_imported` still
  restores missing blobs and re-adopts scopes (`import --no-media` then `import` works).
- **Keychain**: macOS `security -i` with commands on stdin so the password is never an argv.
  Elsewhere: `CHATSTORE_PASSWORD_FILE` (0600) or a tty prompt.

## CLI

- Versioned envelope `chatstore-cli-v1`; stable exit codes 0–9; bounded output with cursors.
- Date-only `--until` is exclusive at local midnight in `config.timezone` (an implicit +1 day was
  removed; the DST test now checks 23 h/25 h days with explicit consecutive dates).
- `--context` for exports defaults to `full`.
- `media status` reports `restored_blob_files` and `local_state`; JSON field names are kept stable
  when adding fields (`availability` remains; `local_state` was added).

## Code structure

- `cli/common.py` holds the typed CLI contract so `extra.py` no longer reaches into `app` via an
  untyped indirection.
- `SourceObservation` NamedTuple replaces 11-element positional tuples.
- `Cache.set_head`, `add_revision_row`, `merge_observations` replace raw SQL in curation and import,
  so record transitions live in the cache module.
- Export's minimal/full context paths share one tail; `media.py` owns blob location.

## Measurements

| Operation | Result |
| --- | --- |
| Initial sync | WhatsApp ~230 s → 119 s after bulk mode; Messages ~90 s |
| Incremental no-op sync | ~9–12 s per source |
| Full text export | 272,981 messages → 81 archives, 4m51s–6m20s |
| Restore into empty dir | 3m52s–5m17s; identical `(urn, revision_digest)` set, 0 conflicts |
| One month with media | 803 blobs, 496 MB; export 30 s, import 25 s |
| Final round trip after review fixes | 662,299 identical records; revisions 667,766; FTS 227,542 |

## Deferred / known gaps

- Cache size (~1.9 GB for 661k records): record JSON is stored twice (current + revision) plus
  FTS. Needs a schema migration to store revisions as the only copy.
- Export speed (~3.5 s per bucket) is unprofiled.
- `media_by_chat` groups on source availability only; a per-chat local-state view would need a
  filesystem pass per attachment.
- Non-macOS keychain integration; Linux/Windows are archive-only.
- Digital signatures for archives (authorship, as distinct from integrity) are a future feature.
- No PyPI release yet.
