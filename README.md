# chatstore

Local-first tool for ingesting personal message data (WhatsApp for macOS, Apple Messages),
searching it via SQLite FTS5, citing entities through stable URNs, and exporting
password-encrypted monthly archives that can rebuild the cache elsewhere.

Status: Phase 1 (contracts and identity) complete. See [PLAN.md](PLAN.md) for the full plan and
[specs/](specs/) for the durable contracts.

## Layout

```
src/chatstore/            Python package
  adapters/               read-only source adapters (whatsapp, messages)
  canonical/              canonical record model + schema version
  identity/               UUIDv5 URNs, account scopes, aliases
  cache/                  SQLite schema, migrations, FTS5
  archive/                export / verify / import, manifest
  cli/                    command surface + JSON envelope
helpers/messages-decoder/ Rust helper (pinned imessage_database) -> JSONL
specs/                    contracts, JSON Schemas, test vectors
docs/                     security, integration
skills/chatstore/         portable agent skill (CLI client only)
tests/                    unit / integration / synthetic fixtures
```

## Rules

- Never modify source databases. Open read-only; snapshot via SQLite backup API.
- Never commit passwords, raw messages, cache files, or real-data fixtures.
- Never change identity derivation rules silently.
