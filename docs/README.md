# Docs

Final-state documentation. The specs in [../specs/](../specs/README.md) are the durable contracts;
these pages explain the system as built.

| Page | Contents |
| --- | --- |
| [overview.md](overview.md) | Goal, status, what it does, non-goals, requirements and where each is enforced |
| [architecture.md](architecture.md) | Components, module map, storage layout, cache schema, sync, archive export/import pipelines, CLI contract |
| [sources.md](sources.md) | WhatsApp and Apple Messages adapters: what is read, how it is interpreted, incremental strategy |
| [operations.md](operations.md) | Install, configure, diagnose, sync, search, media, archive/restore, curation, scheduling, troubleshooting |
| [security.md](security.md) | Threat model, source access, archive crypto, passwords, Git, retention, agents, helper downloads |
| [integration.md](integration.md) | Using the CLI/JSON from journals, CRMs, agents; citations; exit codes to branch on |
| [decisions.md](decisions.md) | Every trade-off and the evidence behind it, by topic; measurements; known gaps |
| [testing.md](testing.md) | Verification contract, suites, acceptance test, real-data conventions |
| [history.md](history.md) | Delivery phases, review and corrections, publication, plan corrections |
