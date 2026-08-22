# Instructor runbook

## Delivery promise

The room builds one explainable payment-quality incident, not a general payment platform. Learners use their
own Databricks Free Edition workspaces; the instructor workspace supplies canonical rehearsal evidence and
recovery demonstrations only.

## T-24 hours — prepare the machine and repository

1. Start from a clean clone and confirm Python 3.11–3.13, uv, Java 17/21, Databricks CLI 1.13.0, and Git.
2. Authenticate with a named OAuth profile for the exact instructor workspace:

   ```bash
   databricks auth login \
     --host https://dbc-ec01047b-2f32.cloud.databricks.com \
     --profile workshop-instructor
   export DATABRICKS_CONFIG_PROFILE=workshop-instructor
   ```

3. Confirm the selected profile. Never rely on an implicit default profile:

   ```bash
   databricks auth profiles
   databricks current-user me -p "$DATABRICKS_CONFIG_PROFILE"
   ```

4. Run the complete local rehearsal:

   ```bash
   ./scripts/bootstrap.sh
   ./scripts/cache_dqx_wheel.sh
   ./scripts/e2e_local.sh
   ```

5. Inspect `.workshop-evidence/local/` and `docs/instructor/expected-evidence.md`. A local pass is not hosted
   evidence.

## T-12 hours — canonical Free Edition rehearsal

This step deploys and runs dev resources, optionally refreshes the instructor Genie Space, and releases the
incident. Confirm the profile and intended workspace before running it:

```bash
export DATABRICKS_CONFIG_PROFILE=workshop-instructor
./scripts/preflight.sh --runtime
./scripts/e2e_remote.sh --confirm-remote --reset-and-restore
```

The runtime preflight executes `SELECT 1` before any deploy. If it reports
`serverless_compute_unavailable`, stop: do not retry deployment into a workspace that cannot start compute.
Databricks documents that Free Edition fair-use quota can suspend compute until its daily or, in extreme cases,
monthly reset while retaining workspace data and settings. Recheck after the reset or rehearse in another
explicitly selected Free Edition workspace; never silently switch profiles.

Success requires `REMOTE_RESTORE=PASS state=baseline` followed by `REMOTE_E2E=PASS`. This lane proves guarded
reset and redeploys the healthy baseline so the instructor workspace is ready for the room. Review
`.workshop-evidence/remote/`, copy only sanitized evidence into the
documented fallback folders, and record the run/update/space IDs in the rehearsal receipt. Never publish tokens,
profile files, user email addresses, or raw CLI debug output.

After the checkpoint, dual-agent, hosted, fallback, and website receipts are recorded, the final release
assertion is:

```bash
./scripts/readiness.sh
```

It must return `WORKSHOP_READINESS=READY`; any missing named receipt is an explicit failure.

If you intentionally need a reset without the automatic baseline restore, use the guarded command only after
explicit inspection:

```bash
export DATABRICKS_WAREHOUSE_ID=THE_CONFIRMED_WAREHOUSE
./scripts/reset.sh --confirm dbx_agentic_dev
```

## Room opening — 30 minutes before

- Open the learner BRD, how-we-work loop, starter tag, prompts, pipeline graph, quarantine results, Gold view, and Genie Code.
- Confirm the SQL warehouse and serverless pipeline are usable.
- Put the exact profile setup command on screen, but never your credential material.
- Ask learners to use their own Free Edition workspace and keep the cheat sheet open.
- Pair anyone still blocked on authentication after ten minutes.
- Keep fallback lanes visibly labeled: local, remote dry-run, deployed, live runtime, or replay.
- Keep the four-entity incident in view. Do not introduce customers, payouts, or a production target.

## Four-hour clock

| Time | Outcome | Instructor checkpoint |
|---|---|---|
| 09:00–09:15 | Own the outcome | Learners can state `m-007` 25 → 45 from the BRD |
| 09:15–09:35 | Contract + plan | Human approves the agent plan before code |
| 09:35–10:10 | dbldatagen story | 100,000 transactions and eight deterministic incidents |
| 10:10–10:50 | Bronze | Four sources retain lineage, evolution, and rescue metadata |
| 10:50–11:45 | DQX + Silver | Invalid records remain queryable and explained |
| 11:45–12:30 | Gold + DABs | Local E2E, human deploy gate, hosted baseline |
| 12:30–13:00 | Genie Code + replay | Four questions, risk change 25 to 45, one lesson written |

## Recovery thresholds

- Authentication: ten minutes, then pair the learner.
- Generator/package resolution: seven minutes, then use `data/fallback/generated/`.
- Serverless pipeline start: ten minutes, then use the current sanitized dry-run and results.
- Runtime preflight: fail before deploy; retry after the documented quota reset or explicitly select a separately
  authenticated Free Edition workspace.
- DQX dependency: use only the pinned official dependency or a checksum-reviewed official wheel.
- Genie Code: five minutes, then replay the exact prompts, SQL, and bounded results from the current rehearsal.

Fallbacks keep the class moving; they do not convert into live proof. Capture which learners ran live, paired, or
replayed.

## Safety and facilitator cues

- Pause before deploy, incident replay, and reset. Read the active profile aloud.
- Do not start a second active Free Edition pipeline.
- Keep the four contracted entities only throughout the workshop.
- Never accept `ON VIOLATION DROP ROW` in the workshop pipeline.
- Do not diagnose a merchant as fraudulent; describe observed data-quality and chargeback evidence.
- Cleanup is after delivery, scoped to the exact dev bundle and `dbx_agentic_dev` schema.

## Exit criteria

The workshop is delivery-ready only when the current repository revision has:

- complete local E2E proof;
- strict bundle and hosted pipeline dry-run proof;
- a successful dev deployment, baseline CLI upload, and pipeline update;
- SQL assertions for risk 25 before and 45 after replay;
- four completed Genie questions;
- observed Auto Loader drift/rescue evidence;
- successful module tags and both Claude Code and Codex fresh-start rehearsals;
- a clean final `./scripts/verify.sh --remote` result;
- no credential material in Git or evidence artifacts.
