# Source findings (native-key validation)

Observed on one real macOS machine during Phase 1. Counts only; no identifiers, names,
numbers or message text are recorded here. These are observations, not guarantees about
other installations. Adapters re-verify every assumption at runtime (`chatstore doctor`).

Snapshot method: `sqlite3` URI `mode=ro` + online backup API into a `0600` file in a
`0700` scratch directory outside the repository. Source files were not modified.

## WhatsApp for macOS (`ChatStorage.sqlite`)

Schema: Core Data (`Z*` tables). Readable with ordinary SQLite (3.51).

| Check | Result |
| --- | --- |
| `ZWAMESSAGE` rows | 409,926 |
| Rows with null/empty `ZSTANZAID` | 0 |
| Distinct `(ZCHATSESSION, ZSTANZAID)` | 195,784 |
| Groups with >1 row | 103,079 (sizes 2–7) |
| Groups with differing `ZMESSAGEDATE` | 0 |
| Groups with differing `ZTEXT` | 1 |
| Groups with differing `ZFROMJID` or `ZISFROMME` | 0 |
| Duplicate groups whose rows have `ZSORT >= 0` | 1 |
| Duplicate groups whose rows all have `ZSORT < 0` | 103,078 |
| Logical messages with only `ZSORT < 0` rows | 146,843 |
| Rows with null `ZCHATSESSION` | 3 |
| `ZFROMJID` null when `ZISFROMME = 0` | 0 |

Conclusions:

- The identity tuple `chat JID + sender/direction + stanza ID` uniquely identifies a logical
  message. Row duplication is a local-store artefact (history sync), not distinct messages.
- Duplicate rows collapse into one canonical message. Every contributing `Z_PK` is retained
  in `source_observations`. Differing text across duplicates becomes a revision, not a new
  message.
- `ZSORT < 0` does not mean "hidden" for our purposes; 146,843 logical messages exist only
  in that form. We import them and record `zsort_negative_only=true` as provenance.
- Stanza IDs: mostly 20/22/32 hex chars; 68 legacy rows contain non-hex characters. Treat
  as opaque strings, no normalisation.
- `ZMESSAGEDATE` is integer seconds since 2001-01-01 UTC (Core Data). `ZSENTDATE` is a
  float in the same epoch. Both preserved raw.
- `ZMESSAGETYPE` has 34 distinct values observed; `0` (text) dominates. Unknown codes are
  preserved as `unknown:<n>`.
- JID suffixes seen: `@s.whatsapp.net`, `@lid`, `@g.us`, `@broadcast`, `@status`,
  `@lid.status`, `@bot`. Group members are mostly `@lid` (27,716 vs 2,513 phone JIDs).
- `ZWAMEDIAITEM`: 389,348 rows, only 3,775 have a local path (`Media/...` relative to the
  group container). Metadata does not imply bytes exist.
- Owner detection: `ZFROMJID` is null on every outgoing row, but `ZTOJID` on incoming rows is
  populated for 2,080 rows with exactly one distinct value, a phone JID. That JID is also the most
  frequent `ZWAGROUPMEMBER` (273 of 310 groups) and its `ZWAZACCOUNT` LID is the `ZCONTACTJID` of
  the self-chat and the second most frequent member (256 groups). This is the only in-database
  signal that names the owner.
- Alias evidence: `LID.sqlite/ZWAZACCOUNT` maps 9,734 of 13,924 LIDs to a phone number;
  `ContactsV2.sqlite/ZWAADDRESSBOOKCONTACT` maps 433 contacts with both `ZLID` and
  `ZWHATSAPPID`. These are the only accepted sources of LID↔phone aliases.
- Excluded from ingestion: `Axolotl.sqlite`, `BackedUpKeyValue.sqlite`, `enc-key.dat`,
  `ZWAMEDIAITEM.ZMEDIAKEY`, `ZWAMESSAGEINFO.ZRECEIPTINFO` blobs (session/crypto material).

## Apple Messages (`~/Library/Messages/chat.db`)

Requires Full Disk Access for the launching process; it was granted on this machine.

| Check | Result |
| --- | --- |
| `message` rows | 77,198 |
| Null or duplicate `message.guid` | 0 |
| Duplicate `chat.guid` / `attachment.guid` | 0 |
| `handle (id, service)` duplicates | 0 |
| Handle ids present on >1 service | 184 |
| Messages in more than one chat | 0 |
| Messages with **no** `chat_message_join` row | 61,290 |
| ...of which have `ck_chat_id` | 61,240 |
| Distinct `ck_chat_id` targets among orphans | 8 (4 match an existing `chat.chat_identifier`) |
| `text` null but `attributedBody` present | 12,662 |
| Both null | 156 |
| `date`, `date_read`, `date_delivered` magnitude | all ≥ 1e12 → ns since 2001 |
| Reactions (`associated_message_type` 2000–2005, 3000–3001) | 1,820 |
| Replies (`thread_originator_guid`) | 1,701 |
| Edits (`date_edited > 0`) | 19 |
| Retractions (`date_retracted > 0`) | 0 |
| App/balloon payloads (`balloon_bundle_id`) | 529 |
| `sync_deleted_messages` rows | 16 |
| Attachments with null `filename` | 942 of 1,596 |

Conclusions:

- Source GUIDs are safe identity inputs for message, chat, attachment.
- `ck_chat_id` has the form `<service>;-;<identifier>`. For joined messages its tail matches
  `chat.chat_identifier` in 15,645 of 15,710 cases but never equals `chat.guid`. Chat
  assignment therefore has three provenance states: `join_table`, `inferred_ck_chat_id`,
  `none`. Inferred chats without a `chat` row become stub chats keyed by the `ck_chat_id`
  string, clearly labelled.
- Handle identity is `(service, id)`; the same phone/email on iMessage and SMS is two
  endpoints. Cross-service suggestion happens via normalised-match evidence only.
- Messages from the local user have `handle_id = 0`; the sender is the account owner.
- `attributedBody` decoding is mandatory. Delegated to the Rust helper.
- Nanosecond timestamps exceed 2^53; JSON carries them as decimal strings.

## Validated by full ingestion (Phase 2)

Counts only; both sources ingested end-to-end into a scratch cache.

| Metric | WhatsApp | Messages |
| --- | --- | --- |
| Logical messages | 195,783 (+1 skipped: no chat session) | 77,198 |
| Chats (incl. stubs) | 724 | 644 + 5 stubs |
| Identities | 18,543 + me | 980 handles → 981 identities + me |
| Attachments | 15,542 (3,411 distinct blobs available) | 2,621 (1,819 purged placeholders) |
| Events | 7,243 (system + group changes) | 1,923 (tapbacks, edits, group actions) |
| LID→phone aliases | 9,734 | — |
| Initial sync wall time | ~230 s | ~90 s |
| Incremental no-op sync | ~9 s | ~10 s |
| Spurious revisions on re-sync | 0 | 0 |

Messages helper: 77,047 rows `decoded`, 151 `empty` (no text, no attributed body), 0 errors;
variants: 74,819 normal, 1,826 tapback, 534 app, 19 edited.
