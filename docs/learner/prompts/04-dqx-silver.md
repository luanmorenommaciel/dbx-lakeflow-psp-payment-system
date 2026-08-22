# Module 04 — DQX, Silver, and quarantine

## How we work this slice

1. Own: seven invalid conditions stay queryable in quarantine with `_errors` and `_warnings`.
2. Plan: propose a plan and wait for my approval.
3. Execute: author DQX rules and the Silver/quarantine routes only.
4. Verify: `./scripts/checkpoint.sh 04`
5. Review: reject any plan that drops invalid data or uses `ON VIOLATION DROP ROW`.
6. Lesson: copy `docs/learner/lesson-template.md` to `.workshop-evidence/lessons/m04.md`

Read `AGENTS.md`, the product specification, the contracts, and the DQX/Lakeflow skills. First propose a plan and
wait for my approval.

After approval, create `configs/dqx-rules.yaml`, `pipelines/src/psp-agentic/silver.py`, and
`pipelines/src/psp-agentic/outputs.py`; extend the cumulative local SDP specification. DQX must be the only domain
rule source. Route valid rows to Silver and retain all invalid rows in explanatory quarantine with `_errors` and
`_warnings`. Cover the six transaction conditions and temporal dispute condition exactly. Never use
`ON VIOLATION DROP ROW`. Do not create Gold or deploy. Run `./scripts/checkpoint.sh 04`, explain one quarantined
row, and stop.
