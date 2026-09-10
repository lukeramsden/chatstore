# Canonical model (`chatstore-canonical-v1`)

The canonical model is the durable contract between adapters, the cache, archives, and
consumers. SQLite tables are a rebuildable projection of it.

## Encoding

- One JSONL file per entity type: `records/<entity>.jsonl`, UTF-8, one object per line,
  sorted by `urn` (then `revision_digest`) before writing so payloads are deterministic.
- Every record has `schema: "chatstore-canonical-v1"` and `entity: "<entity table name>"`.
  (`kind` is reserved for per-entity classification, e.g. message kind.)
- Every record has an `urn` (`urn:uuid:...`).
- Timestamps are objects, never bare numbers:

  ```json
  {"raw": "810752205276200320", "unit": "ns", "epoch": "2001-01-01T00:00:00Z", "utc_ms": 1789000000000}
  ```

  `raw` is the source integer/float as a decimal string (no precision loss); `unit` is
  `s`, `ms`, `us`, or `ns`; `epoch` is the source epoch; `utc_ms` is the derived Unix
  millisecond integer for querying (fits in 2^53). `utc_ms` is `null` when `raw` is null.
- Unknown values are `null`. Enumerations that carry unrecognised source codes use
  `"unknown:<code>"`.
- JSON Schemas in `schemas/` are authoritative; this document explains intent.

## Common fields

| Field | Meaning |
| --- | --- |
| `schema`, `entity`, `urn` | as above |
| `source` | `whatsapp`, `messages`, or `null` for source-independent entities (people, links) |
| `account_scope` | scope UUID (null for source-independent entities) |
| `revision_digest` | `sha256:` digest of the record excluding volatile fields |
| `observed_at` | Unix ms when this revision was observed locally (volatile) |
| `sync_run` | URN of the `sync_runs` record that produced it (volatile) |

## Entities

### accounts
`native_key`, `label` (source-observed, e.g. account login), `scope_created_at`.

### identities
Source endpoints. `native_key`, `kind` (`phone`, `email`, `jid_phone`, `jid_lid`, `jid_group`,
`jid_broadcast`, `jid_status`, `jid_bot`, `me`, `unknown`), `address` (the raw handle/JID),
`service` (Messages transport or null), `observed_names` (list of `{name, origin}` where
origin is `push_name`, `contact_card`, `address_book`, `chat_partner_name`), `is_me`.

### people
Source-independent humans. `urn` (v4), `label` (user-assigned), `notes`, `created_at`.

### identity_links
`person_urn`, `identity_urn`, `state` (`confirmed`, `suggested`, `rejected`),
`evidence` (list of `{type, detail}`; types: `manual`, `normalised_phone`, `normalised_email`,
`lid_phone_pair`, `address_book`), `created_at`, `revoked_at`.

### aliases
`old_urn`, `new_urn`, `reason`, `evidence`, `created_at`. Both remain resolvable.

### chats
`native_key`, `kind` (`direct`, `group`, `broadcast`, `status`, `unknown`),
`observed_name`, `service`, `assignment` (`source` | `inferred` | `stub`),
`is_archived`, `created_at` (timestamp object or null).

### chat_memberships
`chat_urn`, `identity_urn`, `role` (`member`, `admin`, `owner`, `unknown`),
`state` (`active`, `left`, `removed`, `unknown`), `first_seen`, `last_seen`, `evidence`.
URN is derived: `entity_type=event`, native key `["membership", chat_key, identity_key]`.

### messages
`native_key`, `chat_urn` (nullable), `chat_assignment` (`join_table`, `inferred_ck_chat_id`,
`source_column`, `none`), `sender_urn`, `is_from_me`, `transport`
(`imessage`, `sms`, `rcs`, `whatsapp`, `unknown`), `kind` (`message`, `system`,
`reaction`, `edit`, `retraction`, `placeholder`, `unknown:<n>`), `sent_at`, `received_at`,
`edited_at`, `retracted_at`, `state` (`sent`, `delivered`, `read`, `played`, `failed`,
`pending`, `unknown`), `reply_to_urn`, `reply_to_native`, `text` (concatenated plain text of
text parts, for FTS), `has_attachments`, `part_count`, `source_flags` (opaque object of
preserved numeric codes), `tombstone` (null or `{reason, evidence}`).

### message_parts
`message_urn`, `index`, `type` (`text`, `attachment`, `mention`, `link`, `location`,
`contact`, `app`, `opaque`), `text`, `attachment_urn`, `payload` (JSON, allowlisted),
`parser_status` (`decoded`, `partial`, `unsupported`, `redacted`).
URN: `entity_type=event`, native key `["part", message_key, index]`.

### events
`kind` (`reaction`, `reaction_removed`, `edit`, `retraction`, `membership_add`,
`membership_remove`, `subject_change`, `icon_change`, `delivery`, `read`, `unknown:<code>`),
`target_urn` (message or chat), `actor_urn`, `at`, `payload` (e.g. `{emoji}`),
`carrier_message_urn` (the source row that carried the event, if any).

### attachments
`native_key`, `message_urn`, `part_index`, `filename` (basename only), `mime_type`, `uti`,
`declared_size`, `availability` (`available`, `not_downloaded`, `missing`, `not_exported`,
`unknown`), `blob_sha256` (nullable), `source_path_hint` (relative, never absolute home path),
`is_sticker`, `kind` (`image`, `video`, `audio`, `document`, `contact`, `location`, `other`).

### blobs
`sha256`, `size`, `mime_type`, `first_seen`. URN: `urn:uuid` v5 with
`entity_type=attachment`, native key `["blob", sha256]`, source `messages`/`whatsapp` of first
observation — but blobs are compared by `sha256`, not URN.

### revisions
`entity_urn`, `revision_digest`, `observed_at`, `sync_run`, `supersedes` (previous digest or
null), `record` (the full record at that revision). Revisions are immutable.

### source_observations
`entity_urn`, `source`, `account_scope`, `native_table`, `native_row_ids` (list),
`native_key`, `minted`, `fingerprint` (sha256 of the relevant raw row fields),
`adapter_version`, `observed_at`, `sync_run`, `present` (`present`, `absent_from_source`,
`deleted_evidence`).

### sync_runs
`urn` (v4), `source`, `account_scope`, `started_at`, `finished_at`, `status` (`complete`,
`partial`, `failed`), `mode` (`initial`, `incremental`, `full_reconcile`, `import`),
`snapshot` (`{files: [{name, sha256, size, mtime}]}`), `checkpoints`, `counts`, `coverage`
(`{earliest_utc_ms, latest_utc_ms, messages}`), `errors` (list of `{code, count, sample}`,
redacted), `adapter_version`, `parser_version`.

## Redaction / allowlist

Exported `payload` and `source_flags` objects contain only fields named in
`schemas/allowlist.json`. Adapters never emit: media encryption keys, receipt info blobs,
session/identity keys, CloudKit tokens, account passwords, or absolute filesystem paths.

## Revision rules

- A record's identity is `urn`; its content identity is `revision_digest`.
- A new digest for an existing URN creates a new `revisions` row and updates the current
  view. Import never overwrites a current record with an older-observed revision from
  another machine without recording a conflict (see `archive-format.md`).
- Observation history is not source edit history and is labelled as such.
