# Overview

`chatstore` is a standalone, local-first tool that ingests personal message data from WhatsApp for
macOS and Apple Messages, indexes it in a user-local SQLite/FTS5 cache, cites every entity through a
stable `urn:uuid:` URN, and exports password-encrypted monthly archives that rebuild the cache on
another machine without either source application installed.

It is generic: it does not depend on a particular workspace, journal, CRM, agent harness, user
identity or Git repository. Consumers integrate through the CLI, its versioned JSON envelope and
stable identifiers. A portable agent skill (`skills/chatstore/SKILL.md`) is an optional client of
that CLI, not a second implementation.

## Status

Released as `v1.0.0`; the contracts in `specs/` are frozen. All four delivery phases described in [history.md](history.md) are
implemented; the final acceptance test (`scripts/acceptance.py`, 17 checks) passes against real
local sources, and a full export → restore round trip of ~662k records yields an identical current
view. Known gaps are listed in [decisions.md](decisions.md#deferred--known-gaps).

## What it does

- Reads WhatsApp's `ChatStorage.sqlite` and Messages' `chat.db` through read-only, consistent
  snapshots. Source databases are never modified.
- Normalises both into one versioned canonical record stream ([specs/canonical-model.md](../specs/canonical-model.md))
  without discarding provenance: native keys, raw timestamps, unknown event codes and opaque
  payloads are preserved and labelled.
- Stores current records plus every observed revision in a rebuildable SQLite cache with FTS5.
- Answers `search`, `read`, `context`, `resolve`, `chats`, `people`, `media` with bounded, paginated
  output and a `chatstore-cli-v1` JSON envelope ([specs/cli-json.md](../specs/cli-json.md)).
- Derives URNs with UUIDv5 over a versioned tuple so identifiers survive refreshes, exports,
  restores and explicit account migrations ([specs/identity.md](../specs/identity.md)).
- Exports WinZip-AES-256 encrypted archives, one per UTC calendar month plus a catalogue, with
  immutable revisions and explicit lineage ([specs/archive-format.md](../specs/archive-format.md)).
- Restores those archives fail-closed into a clean data directory: wrong passwords, tampering,
  invalid records or unsafe containers leave the cache unchanged.
- Makes gaps visible: missing history, unavailable attachments, decoding failures, stale data,
  partial syncs, unresolved references and content conflicts are all reported, never smoothed over.

## Non-goals

- Sending messages or changing read state.
- Cloud message retrieval, browser automation or account login.
- Automatically downloading remote attachments.
- Automatic identity merging based on display names.
- Recovering data no longer present in an available source.
- A graphical interface, hosted service or multi-user server.
- Claiming that a desktop store holds complete account history.

## Requirements the design holds itself to

| Requirement | Where it is enforced |
| --- | --- |
| Read sources without modifying them | Read-only URIs + SQLite backup API snapshots; `test_sources_unmodified` in acceptance |
| Private data stays user-local, out of the repo | Data dir under `~/Library/Application Support/chatstore`; `.gitignore`; privacy grep before commits |
| Provenance preserved | `source_observations`, raw timestamps, `unknown:<n>` kinds, opaque parts |
| Stable identities | Frozen UUIDv5 namespace, published test vectors, aliases, scope mapping |
| Restore without source apps | Import consumes only canonical records; FTS rebuilt from them |
| Bounded machine-readable output | Envelope with `limit`/`cursor`, `max_chars`, exit codes |
| Gaps visible | `partial` runs (exit 5), `availability`/`local_state`, `unresolved_refs`, `conflicts` |

## Reading order

1. This page.
2. [architecture.md](architecture.md) – components, data flow, storage, cache and sync design.
3. [sources.md](sources.md) – what each adapter reads and how it interprets it.
4. [operations.md](operations.md) – install, sync, archive, restore, curate, troubleshoot.
5. [security.md](security.md) – threat model and operational rules.
6. [integration.md](integration.md) – using the CLI/JSON from journals, CRMs and agents.
7. [decisions.md](decisions.md) – every trade-off and the evidence behind it.
8. [testing.md](testing.md) and [history.md](history.md).
9. [../specs/](../specs/README.md) – the durable contracts.
