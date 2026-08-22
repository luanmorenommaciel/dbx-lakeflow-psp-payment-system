# Module 01 — contract and plan

Read `AGENTS.md`, `CLAUDE.md`, `docs/specs/psp-payment.md`, and
`configs/contracts/agent-contract.yaml`. Use the installed Databricks skills to verify assumptions, but do not
make any remote call.

First propose a concise implementation plan and wait for my approval. After approval, create only:

- `configs/contracts/psp-payment.contract.yaml`;
- `configs/contracts/incident-ledger.yaml`.

Encode the four entities, exact 100,000-transaction story, valid currencies `USD`, `GBP`, `CAD`, `AUD`, seven
invalid conditions, and the held valid chargeback. Do not write pipeline code yet. Run
`./scripts/checkpoint.sh 01`, explain the evidence, and stop.
