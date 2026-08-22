# DBX Agentic Development — progressive four-hour build

## What you will deliver

You own one payment-quality incident. You will give Claude Code or Codex the business requirement, approve its
plan, and progressively build every product artifact: contracts, synthetic data, Bronze, DQX-backed
Silver/quarantine, Gold, a dev bundle, and Genie Code investigation assets. Each learner or pair deploys to its
own Databricks Free Edition workspace.

The repository supplies the safe harness—agent authority, module prompts, tests, fallback data, and recovery
tags—not a product to copy. Each module is one lap of the same loop:

```text
own the outcome → write the plan → execute the slice → verify the checkpoint
                → human review → teach the house → evidence in the center
```

Read [the BRD](brd-psp.md), [foundations](foundations.md), [techs](techs.md), and [how we work](how-we-work.md)
before you paste a prompt.

## Before the clock starts

Complete [setup.md](setup.md). Start from the immutable starter and create your own branch:

```bash
git switch -c student/<name> workshop-v1-starter
export DATABRICKS_CONFIG_PROFILE=dbx-workshop
./scripts/bootstrap.sh
```

Expected final line: `BOOTSTRAP=READY`. Authentication trouble lasting ten minutes triggers pairing with a green
learner. Package resolution or generation lasting seven minutes triggers committed fallback data. Never copy
credentials, and never represent instructor replay as your own live proof.

For every module, open the linked prompt and paste it unchanged into Claude Code or Codex. Claude Code starts in
planning mode with `claude --permission-mode plan`; Codex follows the same repository contract. The agent must
propose a plan, wait for you, implement only after approval, pass the checkpoint, and stop. Commit your work after
each pass. Copy [lesson-template.md](lesson-template.md) to `.workshop-evidence/lessons/mNN.md` after each
checkpoint.

## 09:00–09:15 — Own the outcome

You are the named owner of NovaPay (`m-007`). Risk operations cannot explain rejected payments, and a confirmed
chargeback did not move this merchant's score. Read the BRD first, then foundations, techs, and how we work.

Owned outcome for the day: seven explained quarantine causes, and after the valid replay `m-007` moves from
risk 25 (`normal`) to 45 (`elevated`). Chat is not evidence.

## 09:15–09:35 — Module 01: contract and plan

Input: [01-contract-plan.md](prompts/01-contract-plan.md).

You and the agent translate [the business requirement](brd-psp.md) and
[the product specification](../specs/psp-payment.md) into the payment contract and incident ledger. Review the
proposed entities, currencies, counts, seven invalid conditions, and held valid chargeback before approving
implementation.

```bash
./scripts/checkpoint.sh 01
cp docs/learner/lesson-template.md .workshop-evidence/lessons/m01.md
git add configs/contracts
git commit -m "workshop: complete module 01 contract"
```

Required evidence: `CHECKPOINT=PASS module=01`. The checkpoint proves the encoded contract, not data generation.

## 09:35–10:10 — Module 02: deterministic synthetic data

Input: [02-synthetic-data.md](prompts/02-synthetic-data.md).

The agent builds the seeded dbldatagen generator for merchants, orders, transactions, and disputes. You approve
the plan, then inspect logical counts and deterministic incident assignments.

```bash
./scripts/checkpoint.sh 02
./scripts/generate_fallback.sh
uv run python -m json.tool data/fallback/manifest.json | head -60
cp docs/learner/lesson-template.md .workshop-evidence/lessons/m02.md
git add gen/synthetic
git commit -m "workshop: complete module 02 synthetic data"
```

Required evidence: exactly 100,000 transactions in the complete story and eight incidents: seven invalid
conditions plus one delayed valid chargeback. If generation exceeds seven minutes, use the committed fallback.

## 10:10–10:50 — Module 03: Bronze with lineage and rescue

Input: [03-bronze.md](prompts/03-bronze.md).

The agent builds four streaming Bronze tables using current Spark Declarative Pipelines APIs. Review Auto Loader
schema tracking, `addNewColumns`, `_rescued_data`, and lineage before accepting the graph.

```bash
./scripts/checkpoint.sh 03
cp docs/learner/lesson-template.md .workshop-evidence/lessons/m03.md
git add pipelines/src/bronze.py pipelines/spark-pipeline.yaml
git commit -m "workshop: complete module 03 bronze"
```

Required evidence: four Bronze nodes and `CHECKPOINT=PASS module=03`. A local graph dry-run proves graph
construction; hosted drift is proven later.

## 10:50–11:45 — Module 04: DQX, Silver, and quarantine

Input: [04-dqx-silver.md](prompts/04-dqx-silver.md).

The agent writes the DQX rule source and the valid/quarantine routes. Reject any plan that drops invalid data or
duplicates business rules outside DQX.

```bash
./scripts/checkpoint.sh 04
cp docs/learner/lesson-template.md .workshop-evidence/lessons/m04.md
git add configs/dqx-rules.yaml pipelines/src pipelines/spark-pipeline.yaml
git commit -m "workshop: complete module 04 dqx silver"
```

Required evidence: six transaction failures and one temporal-dispute failure retained with explanatory
`_errors`/`_warnings`; no `ON VIOLATION DROP ROW`.

## 11:45–12:30 — Module 05: Gold, DABs, and dev deployment

Input: [05-gold-dabs.md](prompts/05-gold-dabs.md).

The agent completes merchant-grain risk, one serverless pipeline, and a host-neutral dev-only bundle. First finish
the local story:

```bash
./scripts/checkpoint.sh 05
./scripts/e2e_local.sh --module05
cp docs/learner/lesson-template.md .workshop-evidence/lessons/m05.md
git add databricks.yml pipelines
git commit -m "workshop: complete module 05 gold and dabs"
```

Required local evidence includes `m-007` at 25 before replay and 45 after replay. Return to the human gate before
remote mutation. Confirm the selected profile is your own workspace, then approve commands one at a time:

```bash
databricks auth profiles
databricks current-user me -p "$DATABRICKS_CONFIG_PROFILE"
./scripts/preflight.sh --runtime
databricks bundle validate --strict -t dev -p "$DATABRICKS_CONFIG_PROFILE"
databricks bundle deploy -t dev -p "$DATABRICKS_CONFIG_PROFILE"
databricks pipelines dry-run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE"
./scripts/upload_fallback.sh baseline
databricks bundle run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE" --full-refresh-all
./scripts/verify_remote_data.sh baseline
```

Required hosted evidence: Unity Catalog tables/lineage, preserved DQX quarantine, and baseline risk 25. A hosted
start delay beyond ten minutes switches to explicitly labelled instructor evidence.

## 12:30–13:00 — Module 06: Genie Code and incident delivery

Input: [06-genie-delivered.md](prompts/06-genie-delivered.md).

The agent creates four bounded questions and expected factual results. Run the artifact checkpoint, then open
Genie Code in Databricks and use it to investigate the tables. Inspect the SQL behind every answer.

```bash
./scripts/checkpoint.sh 06
./scripts/release_incident.sh --remote
./scripts/verify_remote_data.sh replay
./scripts/verify.sh --remote
cp docs/learner/lesson-template.md .workshop-evidence/lessons/m06.md
git add docs/genie
git commit -m "workshop: complete module 06 genie delivery"
```

Required outcome: the four questions are answered, lineage and quarantine are inspectable, and the valid replay
changes `m-007` from 25 (`normal`) to 45 (`elevated`). Quality evidence is not proof of fraud. Genie Code is
required; the instructor's reproducible Genie Space is optional enhancement material.

Finish with a three-minute walkthrough that labels each fact as local, deployed, live runtime, or fallback.

## Recovery

Your branch is never overwritten. If a module cannot recover, branch from the latest completed curriculum tag:

```bash
git switch -c student/<name>-recovery workshop-v1-m03-bronze
```

Choose the tag matching your last completed module and replay only your later commits. The curriculum tags are
`workshop-v1-starter`, `workshop-v1-m01-contract-plan`, `workshop-v1-m02-synthetic-data`,
`workshop-v1-m03-bronze`, `workshop-v1-m04-dqx-silver`, `workshop-v1-m05-gold-dabs`,
`workshop-v1-m06-genie-delivered`, and `workshop-v1-solution`.

Cleanup is destructive and outside the four hours. Only the instructor runs the full reset-and-restore
certification lane.
