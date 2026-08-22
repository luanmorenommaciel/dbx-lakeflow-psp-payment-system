# Workshop cheat sheet

Read first: [BRD](brd-psp.md) · [foundations](foundations.md) · [techs](techs.md) · [how we work](how-we-work.md)

| How we work | Move |
|---|---|
| Own | State the `m-007` 25 → 45 outcome before pasting a prompt |
| Plan | Agent proposes; you approve before files change |
| Execute | Current module prompt only |
| Verify | `./scripts/checkpoint.sh 01` through `06` |
| Review | Inspect evidence; you sign deploy |
| Lesson | `cp docs/learner/lesson-template.md .workshop-evidence/lessons/mNN.md` |

| Goal | Command |
|---|---|
| Start learner branch | `git switch -c student/<name> workshop-v1-starter` |
| Module checkpoint | `./scripts/checkpoint.sh 01` through `06` |
| Setup preflight | `./scripts/preflight.sh` |
| Runtime preflight before deploy | `./scripts/preflight.sh --runtime` |
| Local SDP graph | `./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml` |
| Strict DAB check | `databricks bundle validate --strict -t dev -p "$DATABRICKS_CONFIG_PROFILE"` |
| Deploy | `databricks bundle deploy -t dev -p "$DATABRICKS_CONFIG_PROFILE"` |
| Upload baseline | `./scripts/upload_fallback.sh baseline` |
| Run baseline | `databricks bundle run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE" --full-refresh-all` |
| Assert hosted baseline | `./scripts/verify_remote_data.sh baseline` |
| Release valid replay | `./scripts/release_incident.sh --remote` |
| Assert hosted replay | `./scripts/verify_remote_data.sh replay` |
| Upload drift fixture | `./scripts/upload_fallback.sh drift` |
| Assert rescued drift | `./scripts/verify_remote_drift.sh` |
| Final read-only gate | `./scripts/verify.sh --remote` |
| Recovery | `git switch -c student/<name>-recovery workshop-v1-mNN-...` |

Use Genie Code for the required four-question investigation. Genie Space creation is optional instructor
enhancement material. Label evidence as local, remote dry-run, deployed, live runtime, or fallback; never
substitute one for another.
