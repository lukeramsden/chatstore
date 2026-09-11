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
| `doctor` | `{data_dir, python, sqlite, fts5, sources: [{source, path_found, readable, schema_ok, schema_version, permission_hint, wal_present}], helper: {found, version}, keychain: bool}` |
| `init` | `{data_dir, created: bool, scopes: [{source, scope}]}` |
| `sync --source all|whatsapp|messages [--mode incremental|full]` | `sync_runs` record(s) |
| `status` | `{data_dir, scopes, sync_runs_latest, counts, coverage}` |
| `search <query> [--since --until --source --chat --sender --person --transport --has-attachment --advanced --history]` | `[{urn, revision_digest, chat_urn, chat_label, sender_urn, sender_label, sent_at, transport, kind, snippet, provenance: {source, scope}}]` |
| `chats [--source --since]` | `[{urn, kind, label, service, message_count, last_message_at, participants}]` |
| `people` | `[{urn, label, identities: [{urn, address, kind, link_state}]}]` |
| `read <chat-urn> [--since --until --limit --cursor]` | `[message summaries with text, parts, attachments, events]` |
| `resolve <urn|citation-json>` | `{urn, status, kind, record, aliases, revisions: [{digest, observed_at}]}` |
| `context <message-urn> [--before N --after N]` | `{target, before: [...], after: [...]}` |
| `archive export [--since --until --media text|available-media --output DIR --force --catalogue-only --no-catalogue]` | `{output, written, archives: [{kind, label, action: written|unchanged|skipped_empty, path, export_id, revision, supersedes, counts, media, coverage}]}` |
| `archive verify <path|dir>... [--no-schema]` | `[{path, ok, wrong_password, problems: [...], counts, schema_errors, manifest}]` (exit 7 if any not ok) |
| `archive import <path|dir>... [--no-media --stop-on-conflict]` | `[{path, action: imported|already_imported|superseded|branch_conflict|invalid|wrong_password|schema_error, export_id, kind, label, revision, outcomes: {inserted, updated, unchanged, older, conflict}, retired, unresolved_refs, blobs_restored, media_not_restored, conflicts, problems, exit_code}]` (exit 6 on branch/content conflicts, 7 on invalid) |
| `archive password suggest|set|status|clear` | `suggest` prints to a tty only and refuses `--json`; `set` → `{stored, service, account}`; `status` → `{keychain, password_file, keychain_available}` |
| `archive list` | `{exports: [...], imports: [...]}` ledgers |
| `rebuild-index` | `{rows}` |
| `scope list|map <from> <to>` | scopes / `{aliases_created}` |
| `identity link|unlink|suggest` | identity_links records |
| `conflicts list|resolve <id> --keep <digest>` | conflicts |
| `purge --entity <urn>|--source <s> --confirm` | `{removed, warning}` |
| `media status [--source --chat --top N]` | `{summary: [{source, availability, count, declared_bytes, hashed, reason}], restored_blob_files, chats_with_most_unavailable: [{chat_urn, chat_label, source, unavailable, not_downloaded, missing, not_exported, unknown, declared_bytes, last_message_utc_ms}], reasons}` |
| `media list [--availability --source --chat --since --until --limit --cursor]` | `[{urn, message_urn, chat_urn, chat_label, sender_urn, sender_label, source, sent_at_utc_ms, availability, kind, mime_type, declared_size, blob_sha256, source_path_hint, filename}]` newest first |

## Dates and zones

- `--since/--until` accept `YYYY-MM-DD` (interpreted in `config.timezone`, shown in
  `freshness.timezone`) or RFC 3339 with offset. `--until` is exclusive.
- Exports always report exact UTC boundaries.

## Query semantics

- Default: the query is a literal phrase (FTS5 quoted string with quotes escaped). Multiple
  words are ANDed as literal tokens.
- `--advanced`: raw FTS5 syntax, syntax errors → exit 2 with a message.
- Reactions and system events are excluded unless `--kind` includes them.
- `--history` also searches retained revisions (flagged `revision_superseded` in results).
