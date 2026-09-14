# CLI JSON contract (`chatstore-cli-v1`)

## Envelope

Every command with `--json` prints exactly one JSON object on stdout:

```json
{
  "envelope": "chatstore-cli-v1",
  "command": "search",
  "ok": true,
  "exit_code": 0,
  "data": {...} | [...],
  "page": {"limit": 50, "returned": 50, "next_cursor": "opaque" | null, "truncated": true},
  "freshness": {
    "sources": [
      {"source": "whatsapp", "scope": "...", "last_sync_finished_at": 1789000000000,
       "last_sync_status": "complete", "coverage": {"earliest_utc_ms": ..., "latest_utc_ms": ..., "messages": 195784}}
    ],
    "timezone": "Europe/London"
  },
  "warnings": [{"code": "partial_sync", "message": "..."}],
  "errors": [{"code": "...", "message": "..."}]
}
```

- `data` is bounded: default `--limit 50`, max `--limit 500`; `read`/`context` cap text at
  `--max-chars 20000` and mark `truncated`.
- Cursors are opaque base64 of `(sort key, urn)`; ordering is deterministic
  (`utc_ms`, then `urn`).
- Human output (no `--json`) is derived from the same data and never adds fields.
- Timestamps in `data` use the canonical timestamp object plus `iso` (UTC RFC 3339) for
  convenience.
- Messages bodies are never written to stderr or logs. `--json` never contains passwords.

## Exit codes

| Code | Meaning |
| --- | --- |
| 0 | success |
| 1 | generic runtime failure |
| 2 | usage error (argparse) |
| 3 | permission / source access failure (e.g. Full Disk Access missing) |
| 4 | source or archive schema incompatible (fail closed) |
| 5 | partial success (sync incomplete, partial restore) — `data` still emitted |
| 6 | conflict requires user resolution |
| 7 | invalid archive (auth failure, tamper, checksum, unsafe member, limits) |
| 8 | not found / unresolvable URN |
| 9 | data directory not initialised |

## Commands and `data` shapes

| Command | `data` |
| --- | --- |
| `version` | `{version, envelope, python, sqlite, platform}` |
| `doctor` | `{data_dir, python, sqlite, fts5, sources: [{source, path_found, readable, schema_ok, schema_version, permission_hint, wal_present}], helper: {found, version}, keychain: bool}` |
| `init` | `{data_dir, created: bool, scopes: [{source, scope}]}` |
| `sync --source all|whatsapp|messages [--mode incremental|full]` | `sync_runs` record(s) |
| `status` | `{data_dir, scopes, sync_runs_latest, counts, coverage}` |
| `search <query> [--since --until --source --chat --sender --person --transport --has-attachment --advanced --history]` | `[{urn, revision_digest, chat_urn, chat_label, sender_urn, sender_label, sent_at, transport, kind, snippet, provenance: {source, scope}}]` |
| `chats [--source --since --label --participant]` | `[{urn, kind, label, service, message_count, last_message_at, participants}]`. `--label` is a case-insensitive substring over the chat label and participant labels; `--participant` is an identity or person URN (a person's linked identities and all aliases are followed). |
| `people` | `[{urn, label, identities: [{urn, address, kind, link_state}]}]` |
| `read <chat-urn> [--since --until --limit --cursor --order asc\|desc]` | `[message summaries with text, parts, attachments, events]`. Oldest first by default; `--order desc` returns newest first (so `--limit N` is the last N) and `next_cursor` continues towards older messages. |
| `resolve <urn|citation-json>` | `{urn, status, kind, record, aliases, revisions: [{digest, observed_at}]}` |
| `context <message-urn> [--before N --after N]` | `{target, before: [...], after: [...]}` |
| `archive export [--since --until --media text|available-media --context full|minimal --output DIR --force --catalogue-only --no-catalogue]` | `{output, written, archives: [{kind, label, action: written|unchanged|skipped_empty (never exported and empty), path, export_id, revision, supersedes, counts, media, coverage}]}` |
| `archive verify <path|dir>... [--no-schema]` | `[{path, ok, wrong_password, problems: [...], counts, schema_errors, manifest}]` (exit 7 if any not ok) |
| `archive import <path|dir>... [--no-media --stop-on-conflict]` | `[{path, action: imported|already_imported|superseded|media_failed|branch_conflict|invalid|wrong_password|schema_error, export_id, kind, label, revision, outcomes: {inserted, updated, unchanged, older, conflict}, retired, unresolved_refs, blobs_restored, blobs_failed, media_not_restored, conflicts, problems, exit_code}]` (exit 1 if a blob could not be restored — the archive is not recorded, re-run to retry; 6 on branch/content conflicts; 7 on invalid). Re-importing an already imported archive restores blobs that are still missing. |
| `archive password suggest|set|status|clear` | `suggest` prints to a tty only and refuses `--json`; `set` → `{stored, service, account}`; `status` → `{keychain, password_file, keychain_available}` |
| `archive list` | `{exports: [...], imports: [...]}` ledgers |
| `rebuild-index` | `{rows}` |
| `scope list|map <from> <to>` | scopes / `{aliases_created}` |
| `identity link|unlink|suggest` | identity_links records |
| `conflicts list|resolve <id> --keep <digest>` | conflicts |
| `purge --entity <urn>|--source <s> --confirm` | `{removed, warning}` |
| `media status [--source --chat --top N]` | `{summary: [{source, availability, local_state, count, declared_bytes, hashed, reason}], restored_blob_files, chats_with_most_unavailable: [{chat_urn, chat_label, source, unavailable, not_downloaded, missing, not_exported, unknown, declared_bytes, last_message_utc_ms}], reasons, local_states}`. `availability` is what the source app had when last synced (archived verbatim); `local_state` (`restored` = in this data dir's blob store, `source_file` = the app's file exists here, `absent`) is derived from the filesystem now. |
| `media list [--availability --local-state --source --chat --since --until --limit --cursor]` | `[{urn, message_urn, chat_urn, chat_label, sender_urn, sender_label, source, sent_at_utc_ms, availability, local_state, kind, mime_type, declared_size, blob_sha256, source_path_hint, filename}]` newest first |
| `helper install [--tag vX.Y.Z]` | `{path, tag, asset, sha256, config_updated, requested_tag}` — downloads the release helper for this chatstore version, verifies against the release `SHA256SUMS`, installs to `<data-dir>/bin`, sets `config.helper_path` (exit 1 `helper_install_failed` on any mismatch) |
| `helper status` | `{found, path, chatstore_version, install_dir}` |

## Dates and zones

- `--since/--until` accept `YYYY-MM-DD` (interpreted in `config.timezone`, shown in
  `freshness.timezone`) or RFC 3339 with offset. Both name an instant (a date is local
  midnight at its start); `--until` is exclusive, so `--since 2026-03-01 --until 2026-04-01`
  is exactly March.
- Exports always report exact UTC boundaries.

## Labels

- Identity label: first observed name, else the address; `me` for the own identity.
- Chat label (`chats[].label`, `chat_label` on search/read/media results): `observed_name`; else,
  when the chat has exactly one non-me participant, that participant's label; else the bare handle
  parsed from a composite native key (`["guid","SMS;-;+1555…"]` → `+1555…`); else `native_key`.

## Query semantics

- Default: the query is a literal phrase (FTS5 quoted string with quotes escaped). Multiple
  words are ANDed as literal tokens.
- `--advanced`: raw FTS5 syntax, syntax errors → exit 2 with a message.
- Reactions and system events are excluded unless `--kind` includes them.
- `--history` also searches retained revisions (flagged `revision_superseded` in results).
