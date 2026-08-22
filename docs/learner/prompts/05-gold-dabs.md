# Module 05 — Gold and Databricks Asset Bundles

## How we work this slice

1. Own: merchant-grain Gold shows `m-007` at risk 25 locally; deploy only after I say yes.
2. Plan: propose a plan and wait for my approval.
3. Execute: complete Gold, the `dev` bundle, and the cumulative local graph.
4. Verify: `./scripts/checkpoint.sh 05`
5. Review: I sign every remote command; do not choose a Databricks profile for me.
6. Lesson: copy `docs/learner/lesson-template.md` to `.workshop-evidence/lessons/m05.md`

Read `AGENTS.md`, the product specification, all contracts, and the DABs, Lakeflow, and Unity Catalog skills.
First propose a plan and wait for my approval.

After approval, create `pipelines/src/gold.py`, `databricks.yml`, and
`pipelines/resources/pipeline.yml`; complete the cumulative local SDP specification. Keep Gold at merchant
grain, target only `dev`, use one serverless pipeline, and keep the bundle host-neutral. Run
`./scripts/checkpoint.sh 05` and stop before any remote mutation.

Only after I explicitly approve the deployment, show me each command before running it: strict bundle validation,
deploy, hosted pipeline dry-run, baseline upload, full refresh, and baseline verification in my selected Free
Edition profile. Do not choose a Databricks profile for me and do not reset anything.
