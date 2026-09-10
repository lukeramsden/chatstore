# Identity specification (`chatstore-id-v1`)

Status: frozen. Changing anything in this document is a breaking change and requires a new
version string (`chatstore-id-v2`) alongside this one; v1 URNs must remain resolvable.

## URN form

```
urn:uuid:xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

Lower-case hex. URNs are opaque identifiers, not secrets. UUIDv5 over guessable inputs is not
a privacy-preserving hash.

## Derivation

```
NAMESPACE = 3f14f01c-5cc4-4325-950f-a56812da489b
name      = json(["chatstore-id-v1", source, account_scope, entity_type, native_key])
uuid      = uuid5(NAMESPACE, name)
```

`json(...)` is the compact JSON serialisation of an array of exactly five strings:

- separators `,` and `:` with no whitespace;
- non-ASCII characters emitted as UTF-8, not `\u` escaped;
- the only escaped characters are `"`, `\`, and control characters U+0000–U+001F, which use
  JSON's standard short forms (`\n`, `\t`, `\"`, ...) or `\u00XX`;
- the result is encoded as UTF-8 before hashing.

This is Python's `json.dumps(x, ensure_ascii=False, separators=(",", ":"))` and is trivially
reproducible in other languages.

Every element is NFC-normalised before serialisation. No case folding is applied globally;
source-specific rules below say where it is.

### Fields

| Field | Values |
| --- | --- |
| `source` | `whatsapp`, `messages` |
| `account_scope` | UUID string of the account scope (see below) |
| `entity_type` | `account`, `identity`, `chat`, `message`, `attachment`, `event` |
| `native_key` | source-specific string, see below |

Entities without a native key (`people`, `identity_links`, `sync_runs`, minted fallbacks)
use random UUIDv4 and are not derived.

### Composite native keys

When a native key is made of several parts, it is the compact JSON array of those parts
(same serialisation rules). Consumers never parse native keys; they are opaque.

## Source rules

### Messages (`source = "messages"`)

| Entity | `native_key` |
| --- | --- |
| account | `"store"` (one logical Messages store per scope) |
| identity (remote handle) | `["handle", service, handle_id]` |
| identity (local user) | `["me", account_login]`, or `["me"]` when unknown |
| chat | `["guid", chat.guid]` |
| chat (inferred, no `chat` row) | `["ck_chat_id", ck_chat_id]` |
| message | `message.guid` |
| attachment | `attachment.guid` |
| event (reaction/edit/retraction as a message row) | `message.guid` of the event row |

Normalisation: `service` verbatim (`iMessage`, `SMS`, `RCS`); `handle_id` verbatim except
that e-mail addresses (containing `@`) are lower-cased. Phone numbers are not reformatted.

### WhatsApp (`source = "whatsapp"`)

| Entity | `native_key` |
| --- | --- |
| account | `"store"` |
| identity (remote) | JID verbatim, e.g. `123@s.whatsapp.net`, `456@lid`, `789-1@g.us` |
| identity (local user) | `["me"]` |
| chat | `ZWACHATSESSION.ZCONTACTJID` verbatim |
| message | `[chat_jid, sender, stanza_id]` where `sender` is `"me"` when `ZISFROMME = 1`, else `ZFROMJID` verbatim |
| attachment | `[message_native_key_json, "media", index]` — `index` is `"0"` for the single `ZWAMEDIAITEM` |
| event | the message tuple of the row carrying the event (reactions, edits and system events are rows with stanza IDs) |

Normalisation: JIDs are lower-cased (they are ASCII in practice). Stanza IDs verbatim.
The original observed `ZFROMJID` is used even when an alias later resolves it to another JID.

Validated against real data in `source-findings.md`: `(chat, stanza)` is unique per logical
message with a consistent sender; row duplicates collapse.

## Account scope

A random UUIDv4 minted by `chatstore init` per source, stored in `identity.json`, and
included in every archive manifest and catalogue. It is independent of hostname, path and
installation. Two installations that each ran `init` produce different URNs for the same
source data until one scope is explicitly mapped to the other
(`chatstore scope map <from> <to>`), which records an `aliases` entry per affected URN.

## Minted identifiers

If a trustworthy native key is absent (e.g. a WhatsApp row with a null chat session), the
adapter mints a UUIDv4, records `source_observations.native_key = null`,
`minted = true`, and the matching evidence (row id, fingerprint) so later syncs reuse it.
Ambiguous matches are reported, never auto-merged.

## Entity types per table

| table | `entity_type` in derivation |
| --- | --- |
| identities, accounts | `identity` |
| chats | `chat` |
| messages | `message` |
| attachments, blobs | `attachment` |
| message_parts, events, chat_memberships, aliases | `event` |

`chatstore scope map` relies on this table to re-derive URNs in another scope.

## Aliases

`aliases` map an old URN to a current URN with `reason`, `evidence`, and `created_at`.
Both URNs stay resolvable. Alias reasons: `scope_map`, `lid_phone_pair`, `handle_merge`,
`manual`.

## Revision digest

```
revision_digest = "sha256:" + hex(sha256(canonical_json(record_without_volatile_fields)))
```

`canonical_json` = sorted keys, compact separators, `ensure_ascii=False`, UTF-8. Volatile
fields excluded: `observed_at`, `sync_run`, `revision_digest`. Defined per entity in
`canonical-model.md`.

## Citation contract

```json
{"urn": "urn:uuid:...", "revision_digest": "sha256:..."}
```

`chatstore resolve` returns one of: `current`, `revision_superseded` (URN exists, digest is
an older revision), `tombstoned`, `ambiguous` (alias fan-out), `unknown`, `unavailable`
(entity known via stub only).

## Test vectors

`test-vectors/identity-v1.json` is authoritative and exercised by
`tests/unit/test_identity_vectors.py`. It includes ASCII, non-ASCII (NFC vs NFD input),
composite keys, and e-mail lower-casing cases.
