# Module 02 — deterministic synthetic data

Read `AGENTS.md`, the product specification, both module-01 contracts, and the synthetic-data skill. First
propose a plan and wait for my approval.

After approval, implement only `gen/synthetic/`. Use dbldatagen and seed `22082026` to create merchants, orders,
transactions, and disputes in two batches. Preserve the exact baseline/incident counts and hold the valid
`late-valid-chargeback` for replay. Generated output must be deterministic by logical content even if Spark part
filenames differ. Do not edit the pipeline or deploy. Run `./scripts/checkpoint.sh 02`, explain the manifest and
incident evidence, and stop.
