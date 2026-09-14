# Source adapters

Each adapter implements discovery, diagnostics, snapshotting, extraction, native-key validation,
fingerprinting and coverage reporting, and emits the same versioned canonical records. Counts from
the validation of both adapters against a real machine (numbers only, no content) are in
[specs/source-findings.md](../specs/source-findings.md).

## Consistent, read-only reads (both adapters)

- Databases are opened with `file:…?mode=ro` and copied with SQLite's online backup API, which
  captures committed WAL data. The main file is never copied on its own while the app may write.
- Related databases are snapshotted together where possible; otherwise observation boundaries are
  recorded in the `sync_runs` row.
- Lock waits and snapshot retries are bounded. Writes, checkpoints, permission changes and remote
  media fetches never happen.
- Snapshots live in `<data-dir>/snapshots/` and are removed after ingestion.
- `chatstore doctor` checks the required tables and columns for each source. A mismatch is exit 4
  (incompatible); the adapter refuses to guess.

## WhatsApp for macOS

Source (configurable via `config.source_paths.whatsapp`, verified at runtime):

```text
~/Library/Group Containers/group.net.whatsapp.WhatsApp.shared/ChatStorage.sqlite
```

The native macOS store is an ordinary Core Data SQLite database, readable with stock SQLite.
Schema version is reported as `coredata-z_version-<Z_VERSION>`.

Tables read: `ZWAMESSAGE`, `ZWACHATSESSION`, `ZWAGROUPMEMBER`, `ZWAGROUPMEMBERSCHANGE`,
`ZWAMEDIAITEM`, `ZWAMESSAGEINFO`, plus the LID/phone mapping evidence available locally.

Interpretation rules, each backed by the validation in source-findings:

- **Message identity** is `(chat JID, sender/direction discriminator, stanza ID)`. `ZSTANZAID` was
  present on every row and unique within that tuple.
- **Duplicate rows** from history sync (`ZSORT < 0`) collapse into one logical message; all
  contributing `Z_PK`s are kept in `source_observations`. On the test machine 103k of 196k logical
  messages had duplicate rows; none differed in date, sender or direction.
- **Chat identity** is the chat JID; **identity** is the participant JID. LID (`@lid`) ↔ phone
  (`@s.whatsapp.net`) evidence becomes `aliases` records (~9.7k on the test machine). Identity input
  never changes because an alias later resolves a sender differently.
- **Memberships**: group chats take members from `ZWAGROUPMEMBER`. Direct, status and broadcast
  chats have no rows there, so the adapter synthesises two memberships per chat: the chat's own JID
  (the counterpart, evidence `chat_session_jid`) and the `me` identity (evidence `chat_session_me`).
- **Timestamps** are Core Data seconds since 2001-01-01 UTC, kept raw with declared unit/epoch.
- **Message types and group events** are mapped to canonical kinds where understood; everything
  else is preserved verbatim as `unknown:<n>` and counted under `unsupported`. Nothing is guessed.
- **Media**: `ZWAMEDIAITEM.ZMEDIALOCALPATH` is resolved safely under the group container and the
  file's actual existence checked, giving `availability` ∈ {available, not_downloaded, missing}.
  Metadata never implies bytes exist.
- **Replies** (`ZPARENTMESSAGE`), reactions and system events become `events`/`message_parts`;
  reactions and system events are filterable and not treated as authored messages.
- **Auxiliary rows** (placeholders, rows with no chat session) are skipped and reported. A row with
  no `ZCHATSESSION` marks the run `partial` — one exists on the test machine.
- Credentials, session secrets and media encryption keys are never emitted.

Incremental sync processes logical groups with any `Z_PK` above the checkpoint plus a 5,000-row
look-back window for status changes. A checkpoint moving backwards is reported as a source reset
and forces a full reconcile.

## Apple Messages

Sources:

```text
~/Library/Messages/chat.db
~/Library/Messages/Attachments/
```

The terminal or process running chatstore needs **Full Disk Access**. `doctor` reports the failure
with a hint (`permission_hint`) and exit 3; it never changes permissions or assumes a terminal.

**Decoder helper.** `attributedBody` typedstream blobs, edit history, tapback/variant
classification and attachment rows are decoded by `chatstore-messages-decoder`, a small Rust binary
pinning `imessage-database = "=4.2.0"` (toolchain 1.95 via `rust-toolchain.toml`). It emits
header/footer-framed JSONL (`chatstore-messages-decoder/1`); the adapter fails closed on a format
or exit-code mismatch. It is found via `config.helper_path`, `CHATSTORE_MESSAGES_DECODER`,
`<data-dir>/bin/`, `PATH`, or a development checkout, and can be installed prebuilt with
`chatstore helper install` (see [operations.md](operations.md#apple-messages-helper)). Handles,
chats, joins and chat inference stay in Python SQL.

Interpretation rules:

- **Identity inputs** are source GUIDs (`message.guid`, `chat.guid`, `attachment.guid`), never
  ROWIDs. Handle identity is `(service, id)`; the same phone on iMessage and SMS is two identities
  and cross-service unification is only ever a *suggested* identity link.
- **Bodies**: plain `message.text` is not sufficient (12.6k rows had only `attributedBody`).
  Multipart bodies, reactions, replies (`thread_originator_guid`), edits (`date_edited`),
  retractions and opaque app/balloon payloads are preserved as ordered parts with type labels and
  parser status.
- **Transport** (iMessage / SMS / RCS) is an attribute of the message, not a separate source.
- **Timestamps** are kept as raw integers with unit determined per field; `date`, `date_read`,
  `date_delivered` are nanoseconds since 2001-01-01 UTC. JSON carries `raw` as a decimal string
  because values exceed 2^53.
- **Orphan rows** with no `chat_message_join` (79% of rows on the test machine) are assigned via
  `ck_chat_id` with `chat_assignment = inferred_ck_chat_id`; when no `chat` matches, a stub chat
  keyed by the `ck_chat_id` string is created.
- **Attachments**: metadata does not prove bytes exist; `availability` is checked on disk.
  Inline references to purged attachment rows (`at_<part>_<guid>`; 1,819 of 2,297 on the test
  machine) become `attachments` records with `availability = missing`, keyed `["purged", "<ref>"]`,
  so the message still records that an attachment existed. An attachment row joined to several
  messages is owned by the lowest message ROWID; others reference its URN.
- **Deletion tables** (`sync_deleted_messages` etc.) are read as evidence with documented
  semantics; local absence alone is never an explicit deletion.

Incremental sync re-decodes every row each run and relies on fingerprint comparison, because edits,
read receipts and tapbacks mutate or reference old rows. Decoding the 77k-row test store takes
~1 s; a no-op sync ~10 s.

## Coverage and honesty

- Absence from a snapshot means `absent_from_source`, recorded on the observation; the record
  stays. Only `purge` deletes.
- A desktop store is never claimed to hold complete account history; `status` reports time
  coverage per source and the `sync_runs` row lists unsupported kinds and per-record errors.
