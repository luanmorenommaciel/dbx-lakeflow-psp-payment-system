# DBX Agentic Development contract

This repository has two products: the root four-hour workshop and the existing advanced PSP reference under
`pipelines/src/psp-analytics/`. Read `configs/contracts/agent-contract.yaml` and
`configs/contracts/psp-payment.contract.yaml` before editing.

## Authority

- Begin with a plan and wait for human approval before remote deployment.
- Target only `dev` and never add production resources, schedules, service principals, or classic compute.
- Do not read, print, or commit credentials or Databricks profile contents.
- Keep the existing advanced-reference bundle and pipeline sources unchanged.

## Build contract

- Use `from pyspark import pipelines as dp` for new pipeline code.
- Generate exactly 100,000 deterministic transactions with seed `22082026` using dbldatagen.
- Use the four contracted entities only: merchants, orders, transactions, and disputes.
- DQX is the single source of domain-quality behavior. Preserve invalid records in quarantine with `_errors`
  and `_warnings`; never use `ON VIOLATION DROP ROW` in the agentic pipeline.
- Keep local SDP, Databricks pipeline dry-run, bundle validation, deployment, and runtime evidence distinct.
- Run the relevant Task-Spec evals and repository tests before declaring a unit complete.

## Checkpoints

Stop for human inspection before any deploy, reset, or other remote mutation. Reset may affect only the exact
bundle resources and schema configured for this workshop.
