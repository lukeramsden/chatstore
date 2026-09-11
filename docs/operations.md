# Operations

How to install, run and maintain chatstore. Every command accepts `--json` (envelope in
[specs/cli-json.md](../specs/cli-json.md)) and `--data-dir`.

## Install

Requirements: macOS for ingestion (archive verify/import also runs on Linux/Windows), Python ≥ 3.11,
SQLite with FTS5 (stock macOS Python has it; `doctor` checks).

```sh
pipx install git+https://github.com/lukeramsden/chatstore@v1.0.0   # or pip install …
chatstore init
chatstore helper install          # Apple Messages decoder, prebuilt, sha256-verified
chatstore doctor
```

From a checkout:

```sh
git clone https://github.com/lukeramsden/chatstore && cd chatstore
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
scripts/build-helper.sh           # needs cargo; rust-toolchain.toml pins 1.95
```

### Apple Messages helper

`chatstore helper install [--tag vX.Y.Z]` downloads
`chatstore-messages-decoder-macos-{arm64,x86_64}` and `SHA256SUMS` from the GitHub release matching
the installed chatstore version, refuses to write anything unless the digest matches, installs to
`<data-dir>/bin/` and sets `config.helper_path`. `chatstore helper status` shows which binary would
be used. Builds are produced by `.github/workflows/release.yml` on tags; the workflow refuses a tag
that does not match `pyproject.toml`. WhatsApp ingestion does not need the helper.

### Data directory and configuration

Default `~/Library/Application Support/chatstore` (Linux: XDG data dir). Override with
`--data-dir` or `CHATSTORE_DATA_DIR`. The data directory is never discovered from a repository
config file. `init` creates it (0700), writes `config.json` and `identity.json` with one account
scope per source and an export-set URN.

`config.json`:

| Key | Default | Meaning |
| --- | --- | --- |
| `timezone` | `UTC` | Interprets date-only `--since/--until`; shown in `status` |
| `source_paths` | `{}` | Per-source path overrides (`whatsapp`, `messages`) |
| `search_limit` / `max_limit` | 50 / 500 | Default and maximum page size |
| `max_chars` | 20000 | Output truncation for `context`/`read` |
| `helper_path` | null | Explicit decoder path |

## Diagnose

`chatstore doctor` reports data dir state, Python/SQLite/FTS5, each source (found, readable,
schema version, problems, permission hint) and the helper. Exit 3 means a source exists but is
unreadable (grant Full Disk Access to the terminal for Messages); exit 4 means an unsupported schema
or missing FTS5. `doctor` never changes permissions or installs anything.

## Sync

```sh
chatstore sync [--source all|whatsapp|messages] [--mode incremental|full]
chatstore status
```

- First run is `initial` (minutes: ~4 min WhatsApp, ~1.5 min Messages on a 200k/77k-message
  machine). Later runs are `incremental` (~10 s each).
- `--mode full` reconciles every observation, marking rows no longer in the source
  `absent_from_source`. A detected source reset does this automatically.
- Exit 5 = `partial`: at least one record failed extraction; the `sync_runs` row (`status --json`)
  lists errors and unsupported kinds. Fix or accept, but do not treat the cache as complete.
- Sync never touches the network; sources are opened read-only. You can leave the apps running;
  snapshots are consistent.

## Search and read

```sh
chatstore search "see you tomorrow" --since 2025-01-01 --until 2025-02-01 [--chat urn] [--sender urn] [--person urn]
                 [--kind message,reaction|all] [--has-attachment] [--history] [--advanced] [--limit N] [--cursor C]
chatstore chats | chatstore people
chatstore read <chat-urn> [--since … --until …]
chatstore context <message-urn> --before 5 --after 5
chatstore resolve <urn | '{"urn":…,"revision_digest":…}'>
```

- Date-only arguments are interpreted in `config.timezone`; `--until` is **exclusive** at local
  midnight of that day. Full timestamps accept offsets.
- Queries are literal terms unless `--advanced` (raw FTS5 syntax).
- Default search covers current content of `message` kinds; `--history` adds superseded revisions;
  `--kind all` includes reactions and system events.
- Every result carries `urn`, `revision_digest`, timestamps, sender label and provenance; cite with
  both fields when exact evidence matters. `resolve` distinguishes current, superseded,
  absent-from-source, tombstoned, ambiguous and unknown.
- `rebuild-index` reconstructs FTS from records if it is ever suspected stale.

## Media

```sh
chatstore media status            # per source: availability counts, local_state, chats with most missing
chatstore media list [--chat urn] [--availability …] [--local-state restored|source_file|absent]
```

`availability` is what the source said when last synced (`available`, `not_downloaded`,
`missing`); `local_state` is derived from this machine's filesystem now. A text-only restore shows
`available` + `absent`.

## Archives

### Password

```sh
chatstore archive password suggest        # 256-bit random password; store it in a password manager
chatstore archive password set            # prompts; stored in the macOS keychain via `security -i`
chatstore archive password status|clear
```

Automation: `CHATSTORE_PASSWORD_FILE=<path>` (must be mode 0600). Passwords are never accepted on
argv, never logged and never in JSON. There is no recovery without the password; keep a separate
recovery copy.

### Export

```sh
chatstore archive export [-o DIR] [--since … --until …] [--media text|available-media]
                         [--context full|minimal] [--force] [--catalogue-only|--no-catalogue]
```

Writes `chatstore-<set>-<YYYY-MM>-r000N.zip` per UTC month plus `chatstore-<set>-catalogue-r000N.zip`
into `DIR` (default `<data-dir>/exports`). Unchanged buckets are skipped; a bucket whose content
changed — including one emptied by purge — gets a new immutable revision that supersedes the
previous one. `--media available-media` includes locally available attachment bytes
(content-addressed, deduplicated per archive). `--context minimal` writes stubs instead of
chats/identities (much smaller; requires the catalogue on restore). Publishing is atomic via staging.
Nothing is committed or pushed to Git.

### Verify

`chatstore archive verify <archive-or-dir>…` decrypts, authenticates and fully validates
(manifest, checksums, every record's schema and digest, counts, lineage, blob naming) without
touching the cache. Exit 7 on any problem, with a per-archive problem list.

### Import / restore

```sh
chatstore --data-dir /path/to/fresh archive import <archives…> [--no-media] [--stop-on-conflict]
```

Works on a machine with neither WhatsApp nor Messages. Creates the data dir if needed, adopts the
archive's account scopes and export set, restores records and blobs, rebuilds FTS, and reports per
archive: `proceed` / `already_imported` / `superseded` / `older` / `updated` / `branch_conflict` /
`media_failed`, plus coverage, `unresolved_refs`, `media_not_restored`, `blobs_restored`. Imports
are idempotent; re-running after a failure is always safe. Wrong password or any validation failure
leaves the cache untouched (exit 7). `archive list` shows the export and import ledgers.

## Curation

```sh
chatstore identity suggest                                 # cross-source matches by phone/e-mail; never applied automatically
chatstore identity link <identity-urn>… [--person urn | --label "Name"]
chatstore identity unlink <link-urn>                       # kept as evidence, state=rejected
chatstore scope list
chatstore scope map <from-scope> <to-scope>                # declares two scopes the same account; emits aliases, exported in the catalogue
chatstore conflicts list
chatstore conflicts resolve <id> --keep <revision_digest>  # content conflict
chatstore conflicts resolve <id> --accept                  # archive branch conflict
chatstore purge --entity urn | --source S | --scope SCOPE --confirm
```

Purge is the only operation that deletes data, and its response always warns that already
exported archives and any Git history containing them are unaffected.

## Scheduling

A nightly `sync` and monthly `archive export` via launchd/cron is shown in
[integration.md](integration.md#nightly-sync--monthly-archive-launchd--cron). Check exit codes:
5 means partial, and an export after a partial sync is still an honest export of what was synced.

## Troubleshooting

| Symptom | Meaning / action |
| --- | --- |
| `doctor` exit 3, Messages unreadable | Grant Full Disk Access to the terminal app, restart it |
| `doctor` exit 4 | Source schema changed or FTS5 missing; do not sync; report the schema version shown |
| helper NOT FOUND | `chatstore helper install`, or `scripts/build-helper.sh` from a checkout |
| `sync` exit 5 | Inspect `status --json` → `last_run.errors`; cache is usable but not complete |
| `archive verify` exit 7 with digest problems on old files | Archives written before the Phase 3 "records exported verbatim" fix; re-export |
| `import` `branch_conflict` (exit 6) | Two export lineages for one bucket; inspect `conflicts list`, then `--accept` deliberately |
| `import` `media_failed` | Blob write failed; nothing recorded; fix disk/permissions and re-run |
| Cache large (~3 KB/record) | Known gap: records stored as current + revision copies; see decisions |
