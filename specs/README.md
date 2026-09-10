# Specifications

These are the durable contracts. SQLite tables and FTS indexes are not.

- `canonical-model.md` – entities, JSONL representation rules, schema versioning.
- `identity.md` – UUIDv5 URN derivation (frozen), account scopes, aliases, citation contract.
- `archive-format.md` – bucket rules, container layout, manifest, revisions, safety limits.
- `cli-json.md` – JSON envelope, pagination, exit codes, command data shapes.
- `source-findings.md` – native-key validation results against real sources (counts only).
- `schemas/` – JSON Schemas, generated from `chatstore.canonical.schemas` (tests enforce sync).
- `test-vectors/` – published derivation vectors; changing these is a breaking change.
