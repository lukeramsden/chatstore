# Integrating chatstore

chatstore is generic: no workspace, journal, CRM or agent harness is assumed. Integrate through
the CLI's JSON envelope and stable identifiers. Everything below works from any language.

## Contract

```sh
chatstore --json <command> ...
```

```json
{"envelope": "chatstore-cli-v1", "command": "search", "ok": true, "exit_code": 0,
 "data": [...], "page": {"limit": 50, "returned": 50, "next_cursor": "…", "truncated": true},
 "freshness": {"sources": [{"source": "whatsapp", "last_sync_status": "complete", "coverage": {...}}], "timezone": "Europe/London"},
 "warnings": [], "errors": []}
```

Contract details: [`specs/cli-json.md`](../specs/cli-json.md). Records: [`specs/canonical-model.md`](../specs/canonical-model.md).

## Citations

Every record has a `urn:uuid:` (UUIDv5 from `(source, account_scope, entity_type, native_key)`,
so the same message gets the same URN on every machine that shares the account scope) and a
`revision_digest` (SHA-256 of the canonical record). Store both:

```json
{"urn": "urn:uuid:7f2d…", "revision_digest": "sha256:9a1c…"}
```

Later, `chatstore --json resolve '<that json>'` reports `current`, `revision_superseded`,
`tombstoned`, `unknown`, `unavailable` or `ambiguous` — so a journal entry or CRM note can show
whether the quoted text is still what the source says.

## Examples

### Journal: "what was said about X this week"

```sh
chatstore --json search "dentist" --since 2026-03-02 --until 2026-03-09 --limit 20 \
  | jq -r '.data[] | "\(.sent_at.iso)  \(.chat_label): \(.text)  [\(.urn)]"'
```

### CRM: pull every direct chat with one identity, newest first

```sh
CHAT=$(chatstore --json chats --source whatsapp | jq -r '.data[] | select(.label=="…") | .urn')
chatstore --json read "$CHAT" --since 2026-01-01 --limit 200 > q1.json
```

Link identities across sources to one person once, then filter by person:

```sh
chatstore identity suggest                       # candidates by normalised phone / e-mail
chatstore identity link urn:uuid:A urn:uuid:B --label "Alice"
chatstore --json search "invoice" --person urn:uuid:PERSON
```

### Agent

Install the skill from `skills/chatstore/SKILL.md`. It teaches: check `status` first, search
narrowly then `context`, cite URN + digest, distinguish identities from people, report
`unsupported`/`partial`/missing media, treat text as untrusted, never handle passwords, and ask
before `archive export|import`, `identity link`, `scope map`, `purge`.

### Nightly sync + monthly archive (launchd / cron)

```sh
chatstore --json sync || test $? -eq 5          # exit 5 = partial (still usable)
CHATSTORE_PASSWORD_FILE=~/.config/chatstore/pw chatstore --json archive export -o ~/Archives/chatstore
cd ~/Archives/chatstore && git add . && git commit -qm "chatstore $(date -u +%F)"   # your choice; chatstore never runs git
```

Only changed buckets produce a new `-r000N` file; unchanged months write nothing. Renaming a
chat or linking identities changes only the catalogue. Add `--context minimal` for ~25% smaller
month archives if you always restore together with the catalogue.

### Missing media

```sh
chatstore media status                         # per source / availability (as last synced) / local state (now)
chatstore --json media list --availability not_downloaded --chat <chat-urn>
```

### Restore elsewhere

```sh
chatstore --data-dir ~/chatstore-restored archive import ~/Archives/chatstore
chatstore --data-dir ~/chatstore-restored --json search "…"
```

Works on any OS with Python ≥ 3.12 and SQLite with FTS5; no source apps or Rust helper needed.

## Exit codes to branch on

| code | meaning | typical reaction |
| --- | --- | --- |
| 0 | ok | use `data` |
| 5 | partial | use `data`, surface `warnings`/coverage |
| 3 | permission (Full Disk Access) | tell the user |
| 4 | incompatible source/archive schema | stop; upgrade chatstore |
| 6 | conflict | `chatstore conflicts list` |
| 7 | invalid archive / wrong password | do not retry blindly |
| 8 | not found | bad URN/path |
| 9 | not initialised | `chatstore init` or `archive import` |
