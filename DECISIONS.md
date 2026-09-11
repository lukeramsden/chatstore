# Decisions

Resolves PLAN.md "Decisions to confirm before implementation" and records choices made during
delivery. Each entry says what was decided and what evidence drove it.

## From the plan

1. **Package name / distribution** — `chatstore`, Python package with `chatstore` console
   script; Rust helper built from source with a pinned toolchain (`rust-toolchain.toml`).
   Prebuilt binaries are documented as future work, not shipped in v1.
2. **Supported schema range / parser pin** — WhatsApp: the Core Data schema observed in
   `specs/source-findings.md`; `doctor` checks required tables/columns and fails closed
   (exit 4) on mismatch. Messages: `imessage-database = "=4.2.0"` (released 2026-06-18),
   requires Rust ≥ 1.85 (edition 2024 via rusqlite 0.40); we pin 1.95.
3. **Encryption library** — `pyzipper 0.4.0` (released 2026-05-14, depends on pycryptodomex).
   Evaluation results:
   - Round-trip 8 MB payload: OK, streaming read via `open()` works in 64 KiB chunks.
   - Wrong password → `RuntimeError("Bad password")` before any plaintext is returned
     (password-verifier bytes).
   - Byte flip in ciphertext → `BadZipFile("Bad HMAC check")`, **raised only at end of
     stream**. Consequence: import must decrypt fully into staging and treat the payload
     as untrusted until the stream closes without error. This is enforced in
     `archive/import_.py`.
   - Interop: `7zz 25.x` reads pyzipper output (`AES-256 Deflate`) and pyzipper reads
     7zz-written AES-256 archives. macOS `unzip 6.00` cannot read AES ZIPs; documented.
4. **Retention of historical revisions** — yes, all observed revisions are retained by
   default. Purge is explicit.
5. **Attachments excluded by default** — yes: `--media text` is default; `available-media`
   opt-in.
6. **Filenames reveal month/revision** — yes. The filename prefix is derived from the export
   *set* ID, not the account scope, so it does not reveal the source.
7. **Account-scope mapping UX** — `chatstore scope list` and
   `chatstore scope map <from> <to>`; mapping emits `aliases` (reason `scope_map`) and is
   itself exported in the catalogue. No implicit mapping ever.

## Made during delivery

- **Identity namespace** frozen at `3f14f01c-5cc4-4325-950f-a56812da489b` (Phase 1).
- **WhatsApp duplicate rows** (history-sync, `ZSORT < 0`) collapse into one message keyed by
  `(chat JID, sender, stanza ID)`; all row ids kept in `source_observations`.
- **Messages orphan rows** (no `chat_message_join`, 79% of rows on the test machine) are
  assigned via `ck_chat_id` with `chat_assignment = inferred_ck_chat_id`; when no matching
  `chat` exists a stub chat keyed by the `ck_chat_id` string is created.
- **Messages handle identity** is `(service, id)`; cross-service unification is a suggested
  identity link, never automatic.
- **Timestamps** in JSON are `{raw, unit, epoch, utc_ms}` objects; `raw` is a decimal
  string because Messages nanosecond values exceed 2^53.
- **Build environment note**: on the development machine `xcodebuild` in Xcode 26.2 aborts,
  so `cargo` needs `DEVELOPER_DIR=/Library/Developer/CommandLineTools`.
  `scripts/build-helper.sh` detects and applies this; it is not a project requirement.
- **Toolchain updates**: installed Rust 1.95 via `rustup toolchain install` (non-default,
  selected by `rust-toolchain.toml`) and Homebrew `7zip` for interop tests only.

### Phase 2 (ingestion, cache, CLI)

- **Rust helper scope**: `chatstore-messages-decoder` decodes only what needs `imessage_database`
  (typedstream bodies, edit history, tapback/variant classification, attachment rows). Handles,
  chats, joins and `ck_chat_id` inference stay in Python SQL. Output format
  `chatstore-messages-decoder/1`, header/footer framed; the adapter fails closed on format or
  exit-code mismatch. Decoding the full test store (77k rows) takes ~1 s.
- **Messages incremental sync** re-decodes every row each run and relies on fingerprint
  comparison (`unchanged` upserts) rather than a row-id checkpoint, because edits, read receipts
  and tapbacks mutate or reference old rows. Cost ~10 s on 77k rows; acceptable.
- **WhatsApp incremental sync** processes logical groups with any row `Z_PK` above the
  checkpoint plus a 5,000-row look-back window for status changes. Checkpoint going backwards
  is reported as a source reset and forces a full scan.
- **Messages attachment placeholders**: attributed bodies frequently reference attachments
  (`at_<part>_<message guid>`) whose `attachment` row has been purged (1,819 of 2,297 inline
  refs on the test machine). These become `attachments` records with
  `availability = missing`, keyed `["purged", "<ref>"]`, so the message still records that an
  attachment existed.
- **Shared attachment rows**: an `attachment` row can join to several messages (observed once).
  The lowest message ROWID owns the attachment record; other messages reference its URN.
- **FTS tables** are keyed by rowid (`messages_idx.id`, `revisions.id`) — deleting by an
  UNINDEXED column was a full-table scan and made sync superlinear.
- **Partial vs complete**: any per-record extraction error (e.g. a WhatsApp message with no
  chat session — one on the test machine) marks the run `partial` (exit 5). Honest over tidy.
- **Unknown WhatsApp codes** are preserved verbatim as `unknown:<n>` kinds and counted under
  `unsupported`; nothing is guessed. Observed but unmapped on the test machine: message types
  10, 12, 13, 14, 19, 20, 23, 27, 28, 30, 32, 41, 42, 43, 46, 54, 59, 60, 63, 66, 73, 75, 76 and
  many `ZGROUPEVENTTYPE` values.

### Phase 3 (archives)

- **Manifest binding**: `payload_digest` is a digest over `checksums.json` entries rather than of
  the TAR (which contains the manifest itself). Every member checksum is verified before any
  record is read; wrong password, byte flips in the ciphertext, extra ZIP members, ZipCrypto,
  non-regular or path-escaping TAR members, and declared-size/ratio limits all fail closed with
  exit 7 and leave the cache untouched (integration tests cover each).
- **Attachment records are exported verbatim** in `--media text` mode. Rewriting `availability`
  to `not_exported` would break the revision digest, so instead the manifest counts
  `media.not_exported` and import reports `media_not_restored`.
- **Bucket context is duplicated**: each monthly bucket carries the chats, identities,
  memberships and aliases it needs, so a single bucket is interpretable alone. On the test
  machine this makes 81 text-only buckets total ~290 MB (WhatsApp has ~9.7k LID aliases and
  ~19k identities that recur in most buckets). Accepted for v1; a "slim bucket" mode that relies
  on the catalogue is a possible follow-up.
- **Revision numbering** is per `(export_set, kind, label)`. A re-export whose content digest
  (sorted `(urn, revision_digest)` pairs plus media policy) is unchanged writes nothing.
  `--force` writes a new revision anyway.
- **Import lineage**: an archive already covered by a later imported revision is `superseded`
  and not re-applied; an archive for the same bucket whose lineage does not contain the
  already-imported one is a `branch_conflict` (exit 6, recorded in `conflicts`).
- **Keychain**: macOS `security -i` (commands on stdin) so the password is never an argv.
  Non-macOS platforms use `CHATSTORE_PASSWORD_FILE` (must be mode 0600) or a tty prompt.
- **Measured**: full text export of 272,981 messages → 81 archives in 4m51s; restore into an
  empty data dir in 3m52s with an identical current view (same `(urn, revision_digest)` set,
  same revision/observation/FTS counts, 0 conflicts). One month with media: 803 blobs, 496 MB,
  export 30 s / import 25 s.

### Phase 4 (reconciliation, curation, hardening)

- **Source absence is evidence, not deletion.** A full reconcile (`--mode full`, initial sync,
  or an adapter-detected source reset) marks `source_observations.present =
  absent_from_source`; the record and its revisions stay current and searchable. `resolve`
  shows the observation state. Only `purge` removes data.
- **Source-reset detection** (max row id went backwards) switches an incremental run to
  `full_reconcile` automatically so absence is reconciled in the same run.
- **People and links are source-independent** (`source: null`). Link URNs are UUIDv5 over
  `(person_urn, identity_urn)` so re-linking is idempotent; unlink sets `state=rejected` and
  keeps the record as evidence. Suggestions match the last 10 digits of a phone number or an
  exact lower-cased e-mail across *different* sources; they are listed, never auto-applied.
- **Scope mapping** never rewrites records. `scope map <from> <to>` emits `aliases`
  (`reason: scope_map`) for every non-minted record in `<from>` by re-deriving the URN in
  `<to>`, and stores the mapping in `identity.json`. Entity-type table for derivation is in
  `curation.ENTITY_TYPE` and `specs/identity.md`.
- **Conflict resolution**: content conflicts are resolved by choosing a revision digest
  (`--keep`); the chosen revision becomes current and supersedes the previous head. Archive
  branch conflicts are resolved with `--accept`, after which the branch imports normally.
- **Purge** deletes records, revisions, observations, index rows and FTS entries for an entity
  (messages take parts/attachments/events; chats take their messages and memberships) or a
  whole source/scope. The response always carries the warning that exported archives are
  unchanged.
- **Acceptance run** (real sources, both apps quit, `scripts/acceptance.py --media-month
  2026-04`): 17/17 checks after fixing two test-harness issues (sync_runs rows counted as
  non-idempotence; unresolved cross-archive refs compared against 0 instead of against the
  source's own dangling reply/reaction targets, which were 14 on this machine).

### Post-acceptance improvements

- **Sync performance**: `synchronous=NORMAL` under WAL, batched `upsert_many`, and
  `Cache.bulk()` (no WAL autocheckpoint during a run, checkpoint+truncate on exit). Full
  WhatsApp sync 282 s → 119 s; a crash loses at most the last commit and sync re-runs.
- **`--context minimal`** for bucket archives (stubs instead of chats/identities); default
  stays `full` so monthly buckets are independently interpretable. Bucket content digests
  exclude context records, so a chat rename no longer re-exports every month.
- **`media status|list`** report attachment state; see the review section for the split
  between source availability and local state.

### Design review (Ousterhout checklist) — corrections

- **A revision digest identifies content, not an occurrence.** The head of a URN is the most
  recently observed content. Reverting an edit (A → B → A) previously left B current after a
  restore because the second A was dropped as a duplicate row and then treated as an ancestor.
  Import now decides within the archive's own lineage by observation time; ancestry chains are
  no longer walked. Cross-lineage disagreement is still a conflict, never a clock decision.
- **Import completion covers records, `identity.json` and blobs.** Blobs are restored before
  the ledger commit; a failure is `media_failed` (exit 1) and is not recorded, so a re-run
  retries. `already_imported` archives still restore missing blobs and re-adopt scopes, which
  makes `import --no-media` followed by `import` work.
- **One validation boundary** (`archive/verify.load_archive`) recomputes every record's
  `revision_digest`, validates revisions/observations/stubs and manifest shape/lineage, checks
  all manifest counts and blob naming, and hands import a parsed `LoadedArchive`. Container
  authentication proves the bytes are intact; it never proved the producer built valid records.
- **Empty buckets supersede.** Previously exported buckets stay in the export plan; an emptied
  bucket writes an empty `r000N+1` so restores retire its records. Only never-exported empty
  buckets are skipped.
- **Adapters report `scanned_tables`.** Sync reconciled absence only for tables that emitted a
  row, so a completely emptied table was never reconciled.
- **`--until YYYY-MM-DD` is exclusive at local midnight** (no implicit +1 day), as the spec
  always said. The DST test now checks 23 h/25 h days with explicit consecutive dates.
- **Media: `availability` vs `local_state`.** `availability` is the source observation as last
  synced (archived verbatim). `local_state` (`restored` / `source_file` / `absent`) is derived
  from the filesystem at query time and never stored. A text-only restore previously reported
  attachments as "present on this machine".
- **Structure**: `cli/common.py` holds the typed CLI contract (exit codes, `Ctx`, `Result`,
  `CliError`) so `extra.py` no longer reaches into `app` through an untyped indirection;
  `SourceObservation` replaces 11-element positional tuples; `Cache.set_head`,
  `add_revision_row`, `merge_observations` replace raw SQL in curation and import; the export
  minimal/full context paths share one tail; `media.py` owns blob location.

### Deferred / known gaps

- Cache size (~1.9 GB for 661k records) because record JSON is stored twice (current +
  revision) plus FTS; needs a schema migration to store revisions as the only copy.
- Export speed (~3.5 s per bucket) is unprofiled.
- `media_by_chat` still groups on source availability only; a per-chat local-state view would
  need a filesystem pass per attachment.
- Non-macOS keychain, CI matrix and packaging are out of scope for now.
