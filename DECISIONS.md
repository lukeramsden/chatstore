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
