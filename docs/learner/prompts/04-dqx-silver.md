# Module 04 — DQX, Silver, and quarantine

Read `AGENTS.md`, the product specification, the contracts, and the DQX/Lakeflow skills. First propose a plan and
wait for my approval.

After approval, create `configs/dqx-rules.yaml`, `pipelines/src/psp-agentic/silver.py`, and
`pipelines/src/psp-agentic/outputs.py`; extend the cumulative local SDP specification. DQX must be the only domain
rule source. Route valid rows to Silver and retain all invalid rows in explanatory quarantine with `_errors` and
`_warnings`. Cover the six transaction conditions and temporal dispute condition exactly. Never use
`ON VIOLATION DROP ROW`. Do not create Gold or deploy. Run `./scripts/checkpoint.sh 04`, explain one quarantined
row, and stop.
