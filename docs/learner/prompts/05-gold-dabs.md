# Module 05 — Gold and Databricks Asset Bundles

Read `AGENTS.md`, the product specification, all contracts, and the DABs, Lakeflow, and Unity Catalog skills.
First propose a plan and wait for my approval.

After approval, create `pipelines/src/psp-agentic/gold.py`, `databricks.yml`, and
`pipelines/resources/agentic/pipeline.yml`; complete the cumulative local SDP specification. Keep Gold at merchant
grain, target only `dev`, use one serverless pipeline, and keep the bundle host-neutral. Run
`./scripts/checkpoint.sh 05` and stop before any remote mutation.

Only after I explicitly approve the deployment, show me each command before running it: strict bundle validation,
deploy, hosted pipeline dry-run, baseline upload, full refresh, and baseline verification in my selected Free
Edition profile. Do not choose a Databricks profile for me and do not reset anything.
