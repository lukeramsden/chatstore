# Archive format (`chatstore-archive-v1`)

## Files

```
chatstore-<scope8>-2026-08-r0001.zip      monthly bucket (UTC calendar month, [start, end))
chatstore-<scope8>-undated-r0001.zip      records with no usable event time
chatstore-<scope8>-catalogue-r0001.zip    scopes, identities, aliases, people, links, chats
                                          without bucketed messages, and stubs
```

`<scope8>` is the first 8 hex chars of the export ID's *export set*, not the account scope,
so a filename does not reveal which source it belongs to. Filenames reveal month and revision
number; this leakage is documented and accepted (docs/decisions.md, Archive format).

Explicit ranges (`--since/--until`) produce `chatstore-<scope8>-<startZ>-<endZ>-r0001.zip`
with ISO basic UTC timestamps. Bucket boundaries are always encoded exactly in the manifest.

## Container

```
archive.zip
  payload            WinZip AES-256 (AE-2), deflate-compressed, one member
```

`payload` decrypts to an uncompressed POSIX TAR (ustar/pax) containing, in this order:

```
manifest.json
checksums.json
records/accounts.jsonl
records/identities.jsonl
records/aliases.jsonl
records/chats.jsonl
records/chat_memberships.jsonl
records/messages.jsonl
records/message_parts.jsonl
records/events.jsonl
records/attachments.jsonl
records/blobs.jsonl
records/revisions.jsonl
records/source_observations.jsonl
records/sync_runs.jsonl
records/people.jsonl              (catalogue only)
records/identity_links.jsonl      (catalogue only)
records/stubs.jsonl               (only when cross-bucket references exist)
blobs/sha256/ab/abcdef...         (available-media mode only)
```

Rules:

- Legacy ZipCrypto is never written and never accepted on import.
- The ZIP must contain exactly one member named `payload`; anything else is rejected.
- TAR member names must be relative, normalised, unique, contain no `..`, and be regular
  files only. Symlinks, hardlinks, devices and directories are rejected.
- Every TAR member's declared size is bounded by `manifest.limits`; the total declared size
  and the total inflated ZIP size are bounded before extraction begins. Defaults:
  1 GiB payload, 64 MiB per JSONL file, 200:1 compression ratio.
- Nothing from a payload becomes visible to the cache until the AES authentication tag
  verifies **and** `checksums.json` matches every member.
- Passwords: high-entropy (`chatstore archive password-suggest` gives 256-bit base32).
  WinZip AES uses PBKDF2-SHA1 with 1000 iterations; weak passwords are not protected.

## manifest.json

```json
{
  "archive_format": "chatstore-archive-v1",
  "canonical_schema": "chatstore-canonical-v1",
  "id_version": "chatstore-id-v1",
  "export_id": "urn:uuid:...",
  "export_set": "urn:uuid:...",
  "created_at": 1789000000000,
  "kind": "bucket" | "undated" | "catalogue",
  "bucket": {"start_utc_ms": ..., "end_utc_ms": ..., "label": "2026-08"} | null,
  "revision": 1,
  "supersedes": ["urn:uuid:<export_id>", ...],
  "lineage": ["urn:uuid:<export_id of r1>", "... r2", ...],
  "account_scopes": [{"scope": "...", "source": "whatsapp", "label": null}],
  "scope_mappings": [{"from": "...", "to": "...", "created_at": ...}],
  "observation": {"earliest_observed_at": ..., "latest_observed_at": ..., "sync_runs": ["urn:uuid:..."]},
  "coverage": {"status": "complete" | "partial", "notes": [...]},
  "adapter_versions": {"whatsapp": "...", "messages": "...", "messages_decoder": "..."},
  "counts": {"messages": 0, "events": 0, "attachments": 0, "blobs": 0, "...": 0},
  "attachment_policy": "text" | "available-media",
  "media": {"included_blobs": 0, "missing": 0, "not_downloaded": 0, "not_exported": 0},
  "payload_digest": "sha256:...",
  "content_digest": "sha256:...|media=text",
  "limits": {"max_member_bytes": ..., "max_total_bytes": ...},
  "requires": [],
  "members": ["records/messages.jsonl", ...]
}
```

`checksums.json`: `{"<member path>": "sha256:<hex>"}` for every member except itself and
`manifest.json`. `payload_digest` is the SHA-256 over the sorted `name\0checksum\n` lines of
`checksums.json`; it binds the manifest to every other member (the manifest cannot contain a
digest of a TAR that contains the manifest). It is checked after decryption, together with every
member checksum, before any record is read.

## Bucket membership

- A message belongs to the bucket of its `sent_at.utc_ms` (fallback `received_at`); null →
  `undated`.
- `message_parts`, `attachments`, `events` targeting a message, and `revisions` /
  `source_observations` of those records travel with the message's bucket regardless of when
  they were observed.
- Events without a message target use their own `at`.
- With `manifest.context = "full"` (default) each bucket includes the `chats`, `identities`,
  `aliases`, `chat_memberships` needed to interpret it. Cross-bucket message references
  (replies, reaction targets) are URNs plus a `stubs.jsonl` entry `{urn, entity}`.
- With `manifest.context = "minimal"` (`--context minimal`) chats and identities are shipped
  only as stubs (`{urn, entity: chats|identities}`); the bucket is ~25% smaller and depends on
  the catalogue for labels. Import reports unresolved context stubs in `problems`.
- The catalogue holds scopes, mappings, people, identity_links, and all chats/identities.
  A full restore imports catalogue + buckets; a full-context bucket alone is still interpretable.
- A bucket's `content_digest` covers only its **bucketed** records (messages, parts,
  attachments, events, blobs) — not the context it carries. Renaming a chat therefore produces
  a new catalogue revision, not a new revision of every month. The catalogue's digest covers
  everything it holds. `content_digest` also appends `|media=<policy>` and, when not full,
  `|context=<mode>` so different policies are distinct revisions.

## Revisions and lineage

- Archives are immutable. Re-exporting a bucket whose content digest changed produces
  `r000N+1` with `supersedes` naming the previous `export_id` and `lineage` listing the chain.
- Import uses `bucket` + `export_set` + `lineage` to retire current records that belong to
  the superseded revision and are absent from the new one; their revisions are retained.
  Shared entities (chats, identities) are never deleted by a bucket replacement.
- Two archives for the same bucket and export set whose lineages do not include one another
  are a **branch conflict**. Import stops with exit code 6 and records the conflict; the user
  resolves it with `chatstore conflicts resolve`.
- Content conflicts (same URN, different digests, and the archive's revision history does not
  contain the current digest) are recorded in `conflicts` and shown by `chatstore conflicts
  list`. The current view keeps the previously imported record until resolved. Import order
  and clocks never pick a winner across lineages; within one lineage the newer observation
  is the head, so A → B → A restores as A.
- A bucket that was exported before and is now empty is still exported: an empty `r000N+1`
  supersedes the old revision and retires its records on restore. Only never-exported empty
  buckets are `skipped_empty`.
- Validation is one boundary (`archive/verify.load_archive`): container checksums, manifest
  shape (`kind`, `bucket`, `revision`, `lineage` ends in `export_id`, `supersedes ⊂ lineage`,
  `counts`), every `records/*` row (schema, `revision_digest` recomputed from content, revision
  rows match their `record`, observation shape, stubs), manifest counts against payload, and
  content-addressed blob names. Import consumes only a validated archive.
- Import completion means records committed **and** `identity.json` updated **and** every
  blob in the archive present under `blobs/`. Blobs are restored *before* the ledger commit;
  if that fails the archive is `media_failed` (exit 1) and is not recorded, so a re-run
  retries. Re-importing an `already_imported` archive restores blobs still missing (e.g.
  after `--no-media`) and re-adopts scopes.

## Attachments

`text` (default): metadata only. Attachment records are exported verbatim (`availability` is a
statement about the source at observation time, and rewriting it would break the revision
digest); `manifest.media.not_exported` counts blobs that existed but were not shipped. After
import, `chatstore archive import` reports `media_not_restored` for attachments whose blob is
not present locally. `available-media`: also includes
blobs at `blobs/sha256/<2>/<hex>`, deduplicated within the archive, hashed while streaming
from an open file descriptor; a size or hash mismatch during copy fails the export.

## Export safety

- Staging under `<data-dir>/staging/<export_id>/` with mode `0700`; files `0600`.
- Encrypted ZIP is written to `<output>/.tmp-<name>` then renamed into place after a
  verification pass (`archive verify` on the freshly written file).
- Password from OS keychain (`chatstore archive password set`), `CHATSTORE_PASSWORD_FILE`
  (path to a `0600` file), or interactive prompt. Never argv, never env value, never logs.
- No Git operations.
