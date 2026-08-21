# DBX Agentic Development — four-hour build-along

## What you will deliver

In four hours you will build one small, complete payment-quality story in your own Databricks Free Edition
workspace. A deterministic generator writes merchants, orders, transactions, and disputes. A Lakeflow Spark
Declarative Pipeline moves them through Bronze, DQX-backed Silver/quarantine, and one Gold merchant-risk view.
Databricks Asset Bundles deploy the dev resources. A Genie Space answers four bounded questions, then a late valid
chargeback changes merchant `m-007` from risk 25 to 45.

This is a build-along, not a copy/paste race. At every checkpoint, distinguish local proof, remote validation,
deployed resources, and live runtime evidence.

## Before the clock starts

Complete [setup.md](setup.md), open a terminal at the repository root, and confirm the exact profile you created:

```bash
export DATABRICKS_CONFIG_PROFILE=dbx-workshop
databricks auth profiles
databricks current-user me -p "$DATABRICKS_CONFIG_PROFILE"
./scripts/bootstrap.sh
```

Expected final line: `BOOTSTRAP=READY`. If authentication is not ready after ten minutes, pair with another
learner and keep your own terminal for local work. Do not copy another person's credentials.

## 00:00–00:15 — Frame the incident and prove the toolchain

Read these files in order:

1. `configs/contracts/psp-payment.contract.yaml` — the data promise.
2. `configs/contracts/incident-ledger.yaml` — seven quarantines and one delayed replay.
3. `configs/contracts/agent-contract.yaml` — the agent's authority boundary.
4. `AGENTS.md` and `CLAUDE.md` — equivalent entry points for Codex and Claude Code.

Run the local gate:

```bash
./scripts/verify.sh --local
```

Expected evidence: tests and Ruff pass, the local SDP graph resolves, and the final line is
`VERIFY=PASS mode=--local`. A local pass does not prove your workspace can deploy.

### Agent checkpoint

Start Codex from the repository root, or start Claude Code in planning mode:

```bash
claude --permission-mode plan
```

Give the agent this bounded request:

> Read AGENTS.md and the canonical contracts. Explain the four-entity data journey and propose one dev-only
> change. Do not deploy, reset, alter the advanced reference, or drop invalid rows.

Approve a plan only if it preserves quarantine, targets `dev`, and keeps `pipelines/src/psp-analytics/**`
untouched.

## 00:15–00:45 — Generate a deterministic payment story

Inspect `gen/synthetic/generator.py` and find the dbldatagen specifications. Then generate the learner fallback:

```bash
./scripts/generate_fallback.sh
uv run python -m json.tool data/fallback/manifest.json | head -60
```

Check the manifest, not file byte hashes. Spark may assign different part filenames while the logical contents
remain identical.

Expected contract:

- seed `22082026`;
- 12 merchants, 100,000 orders, 100,000 transactions, and 102 disputes;
- 98,791 baseline transactions plus 1,209 incident transactions;
- seven invalid incident categories preserved for quarantine;
- `late-valid-chargeback` held back until replay.

Checkpoint question: why is the late chargeback valid data while the temporal dispute is quarantined?

## 00:45–01:20 — Build Bronze with lineage

Open `pipelines/src/psp-agentic/bronze.py`. Trace the four source directories into four streaming tables. Every
row must retain `_batch_id`, `_source_file`, `_ingested_at`, and rescued input.

Ask your agent to explain, then make one small learner-selected improvement inside the workshop scope. Re-run:

```bash
./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml
uv run pytest -q tests/pipeline
```

Expected evidence: the graph contains four Bronze nodes and the command exits successfully. A dry-run proves
graph construction, not processed rows.

## 01:20–02:00 — Route quality with DQX and Silver

Read `configs/dqx-rules.yaml`, then follow the rule application in `pipelines/src/psp-agentic/silver.py` and
`outputs.py`. The design has two outputs for each checked stream:

- valid rows continue to Silver;
- invalid rows remain queryable in quarantine with `_errors` and `_warnings`.

The transaction path must explain null/duplicate IDs, non-positive amount, unauthorized BRL, unknown processor,
and orphan order. The dispute path must explain a close timestamp before its open timestamp.

Run the focused contract tests:

```bash
uv run pytest -q tests/contract tests/pipeline
rg "ON VIOLATION DROP ROW" pipelines/src/psp-agentic || true
```

Expected evidence: tests pass and the search prints no workshop-pipeline match. Do not “fix” bad input by silently
dropping it.

## 02:00–02:10 — Break and recovery decision

If local package resolution or generation has consumed more than seven minutes, use the committed fallback data.
If a hosted pipeline has taken more than ten minutes to start, follow the instructor's current fallback. Label it
as fallback evidence; do not call it a live run.

## 02:10–02:40 — Score one merchant in Gold

Inspect `pipelines/src/psp-agentic/gold.py`. The Gold view stays merchant-grain and intentionally simple:

```text
quality_risk_score     = ceil(quarantine_count / 50)
chargeback_risk_points = chargeback_count * 20
risk_score             = min(100, quality + chargeback points)
```

Run the entire local story, including the replay:

```bash
./scripts/e2e_local.sh
```

Expected evidence includes both lines:

```text
LOCAL_ASSERT=PASS phase=baseline ... risk_score=25 risk_band=normal
LOCAL_ASSERT=PASS phase=replay ... risk_score=45 risk_band=elevated
```

The script preserves previous and replay local state under `/tmp`, records logs in `.workshop-evidence/local/`,
and restores both fallback data and the active local pipeline to baseline. The local replay is a clean full replay because the open-source
runner's catalog is process-local; the hosted path demonstrates the incremental update.

## 02:40–03:15 — Validate, deploy, and run with DABs

Return to the human checkpoint before remote mutation. Confirm that the active host is your own workspace:

```bash
databricks auth profiles
databricks current-user me -p "$DATABRICKS_CONFIG_PROFILE"
./scripts/preflight.sh --runtime
```

Then run one command at a time and retain the output:

```bash
databricks bundle validate --strict -t dev -p "$DATABRICKS_CONFIG_PROFILE"
databricks bundle deploy -t dev -p "$DATABRICKS_CONFIG_PROFILE"
databricks pipelines dry-run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE"
./scripts/upload_fallback.sh baseline
databricks bundle run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE" --full-refresh-all
./scripts/verify_remote_data.sh baseline
```

Expected evidence: a deployed managed Volume and serverless pipeline, deterministic generated files uploaded by
the CLI, four Bronze tables, valid Silver tables, both quarantine tables, and `m-007` at risk 25. Free Edition
permits one active pipeline at a time, so do not start a second workshop pipeline. The direct pipeline lane avoids
an unnecessary Lakeflow Jobs dependency and keeps the build-along visible one command at a time.

If your catalog is not `workspace`, export `WORKSHOP_CATALOG` and pass the matching bundle variable consistently:

```bash
export WORKSHOP_CATALOG=my_catalog
export BUNDLE_VAR_catalog="$WORKSHOP_CATALOG"
databricks bundle validate --strict -t dev -p "$DATABRICKS_CONFIG_PROFILE"
databricks bundle deploy -t dev -p "$DATABRICKS_CONFIG_PROFILE"
```

`BUNDLE_VAR_catalog` applies the same override to later `bundle run`, pipeline dry-run, and reset commands. Keep
`WORKSHOP_CATALOG` exported so the SQL and Genie helpers use the same catalog.

## 03:15–03:45 — Create Genie, investigate, and release the incident

Create the reproducible Genie Space from `docs/genie/space.json`:

```bash
./scripts/create_genie_space.sh
./scripts/test_genie.sh
```

The scripts discover your available SQL warehouse, create the Space under your user folder, ask all four contract
questions, and save responses under `.workshop-evidence/remote/`. Open the Space in the UI and inspect the SQL
behind each answer. Genie must report observed quality/dispute facts, never infer that a merchant committed fraud.

Now release the held valid chargeback and incrementally update the hosted pipeline:

```bash
./scripts/release_incident.sh --remote
./scripts/verify_remote_data.sh replay
./scripts/test_genie.sh
```

Expected change: `m-007` keeps 1,209 quarantined transactions, gains one valid chargeback, and moves from risk 25
(`normal`) to 45 (`elevated`). Use Genie Code to inspect or refine the generated SQL; the serialized Genie Space
remains the shared, tool-independent contract.

## 03:45–04:00 — Deliver and explain

Pair with a neighbor and give a three-minute evidence walkthrough:

1. Show the agent contract and name one forbidden action.
2. Show the four-node Bronze intake and one lineage column.
3. Show one DQX error preserved in quarantine.
4. Show `gold_merchant_risk` before/after facts for `m-007`.
5. Show one Genie answer and its SQL.
6. State which evidence is local, deployed, live runtime, or fallback.

Run the final non-destructive gate:

```bash
./scripts/verify.sh --remote
```

Expected final line: `VERIFY=PASS mode=--remote`.

The instructor's pre-event certification uses
`./scripts/e2e_remote.sh --confirm-remote --reset-and-restore`. That extended lane proves the same journey,
exercises guarded cleanup, and rebuilds the healthy baseline for the room. Learners do not run it during class.

## Optional cleanup after delivery

Cleanup is destructive and is not part of the four-hour build. Only run it for your own workspace after checking
the exact target and schema. The script requires both confirmation text and a SQL warehouse ID:

```bash
export DATABRICKS_WAREHOUSE_ID=YOUR_WAREHOUSE_ID
./scripts/reset.sh --confirm dbx_agentic_dev
```

Never run reset in the instructor's workspace during the session.
