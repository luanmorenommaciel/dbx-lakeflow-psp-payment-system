# DBX Agentic Development workshop

This repository is a four-hour, build-along path for Databricks Free Edition. Learners own one payment-quality
incident: generate a deterministic four-entity story, ingest it through one Lakeflow pipeline, keep bad rows in
DQX quarantine, deploy with Databricks Asset Bundles, and prove that a late valid chargeback moves merchant
`m-007` from risk 25 (`normal`) to 45 (`elevated`).

Start here, in this order:

1. [Learner setup](docs/learner/setup.md)
2. [Business requirement (`brd-psp`)](docs/learner/brd-psp.md)
3. [Four-hour guide](docs/learner/workshop-guide.md)
4. [CLI cheat sheet](docs/learner/cheat-sheet.md)

The student index is [docs/learner/README.md](docs/learner/README.md). Instructors use the
[runbook](docs/instructor/instructor-runbook.md) and [evidence checklist](docs/instructor/expected-evidence.md).

Automated rehearsals: `./scripts/e2e_local.sh` proves the clean-room local story; after explicitly selecting the
correct OAuth profile, `./scripts/e2e_remote.sh --confirm-remote --reset-and-restore` validates, deploys, runs,
creates/tests Genie, proves the hosted incident delta, tests guarded reset, and leaves the workspace restored to
the healthy baseline. Generated evidence stays under ignored `.workshop-evidence/`.

The workshop implementation lives at the repository root: `configs/`, `gen/synthetic/`,
`pipelines/src/psp-agentic/`, `scripts/`, `tests/`, and `docs/learner/`.
