# chatstore

Local-first tool for ingesting personal message data (WhatsApp for macOS, Apple Messages),
searching it via SQLite FTS5, citing entities through stable URNs, and exporting
password-encrypted monthly archives that can rebuild the cache on another machine without
either source app.

Status: all four delivery phases of [PLAN.md](PLAN.md) implemented; the final acceptance test
(`scripts/acceptance.py`) passes against real local sources. Durable contracts live in
[specs/](specs/); trade-offs in [DECISIONS.md](DECISIONS.md).

## Quick start (macOS)

Install from GitHub (needs Python ≥ 3.11 and, for Apple Messages, a Rust toolchain):

```sh
pipx install git+https://github.com/lukeramsden/chatstore      # or: pip install git+https://github.com/lukeramsden/chatstore
```

Or work from a checkout:

```sh
git clone https://github.com/lukeramsden/chatstore && cd chatstore
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
scripts/build-helper.sh              # Rust helper for Apple Messages bodies (needs cargo; rust-toolchain.toml pins the version)
.venv/bin/chatstore init             # data dir: ~/Library/Application Support/chatstore (or --data-dir / CHATSTORE_DATA_DIR)
.venv/bin/chatstore doctor           # Full Disk Access, schema compatibility, helper, FTS5
.venv/bin/chatstore sync             # first sync: minutes; later syncs: seconds
.venv/bin/chatstore search "see you tomorrow" --since 2025-01-01
.venv/bin/chatstore --json context urn:uuid:... --before 3 --after 3
```

Archives:

```sh
chatstore archive password suggest          # 256-bit password; store it in a password manager
chatstore archive password set              # macOS keychain (or CHATSTORE_PASSWORD_FILE=<0600 file>)
chatstore archive export -o ~/Archives      # catalogue + one WinZip-AES-256 ZIP per UTC month, text-only by default
chatstore archive verify ~/Archives
chatstore --data-dir /tmp/fresh archive import ~/Archives    # rebuilds a searchable cache with no source apps
```

Every command takes `--json` and returns the `chatstore-cli-v1` envelope
([specs/cli-json.md](specs/cli-json.md)). Exit codes: 0 ok · 1 failure · 2 usage · 3 permission ·
4 incompatible · 5 partial · 6 conflict · 7 invalid archive · 8 not found · 9 not initialised.

An agent skill that uses only the CLI is in [skills/chatstore/SKILL.md](skills/chatstore/SKILL.md).

## Layout

```
src/chatstore/            Python package
  adapters/               read-only source adapters (whatsapp, messages), snapshotting
  canonical/              canonical record model, JSON schemas, schema version
  identity/               UUIDv5 URNs, account scopes, revision digests
  cache/                  SQLite schema, migrations, FTS5, store, queries
  archive/                container, manifest, export / verify / import, password
  cli/                    command surface + JSON envelope
  sync.py                 sync orchestrator; curation.py: links, scopes, conflicts, purge
helpers/messages-decoder/ Rust helper (pinned imessage_database) -> versioned JSONL
specs/                    contracts, JSON Schemas, test vectors, source findings
docs/                     security, integration
scripts/                  build-helper.sh, acceptance.py (final acceptance test)
skills/chatstore/         portable agent skill (CLI client only)
tests/                    unit / integration / synthetic fixtures (no real data)
```

## Development

```sh
.venv/bin/pytest -q            # synthetic fixtures only; no real data required
.venv/bin/ruff check src tests scripts
.venv/bin/mypy src
.venv/bin/python scripts/acceptance.py --media-month 2026-04   # real sources, temp data dirs, ~25 min
```

## Rules

- Never modify source databases. Open read-only; snapshot via the SQLite backup API.
- Never commit passwords, raw messages, cache files, archives, or real-data fixtures.
- Never change identity derivation rules silently; version them (`chatstore-id-v1`).
- Passwords never on argv, in env values, logs or JSON.
