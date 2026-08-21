# Workshop cheat sheet

| Goal | Command |
|---|---|
| Preflight | `./scripts/preflight.sh` |
| Runtime preflight before deploy | `./scripts/preflight.sh --runtime` |
| Tests | `uv run pytest -q` |
| Local SDP graph | `./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml` |
| Strict DAB check | `databricks bundle validate --strict -t dev -p "$DATABRICKS_CONFIG_PROFILE"` |
| Deploy | `databricks bundle deploy -t dev -p "$DATABRICKS_CONFIG_PROFILE"` |
| Remote graph (after deploy) | `databricks pipelines dry-run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE"` |
| Upload generated baseline | `./scripts/upload_fallback.sh baseline` |
| Run pipeline baseline | `databricks bundle run workshop_pipeline -t dev -p "$DATABRICKS_CONFIG_PROFILE" --full-refresh-all` |
| Complete local rehearsal | `./scripts/e2e_local.sh` |
| Assert hosted baseline | `./scripts/verify_remote_data.sh baseline` |
| Create/update Genie | `./scripts/create_genie_space.sh` |
| Ask all Genie questions | `./scripts/test_genie.sh` |
| Local fallback replay | `./scripts/release_incident.sh --local` |
| Free Edition replay | `./scripts/release_incident.sh --remote` |
| Assert hosted replay | `./scripts/verify_remote_data.sh replay` |
| Instructor hosted E2E | `./scripts/e2e_remote.sh --confirm-remote --reset-and-restore` |
| Final readiness assertion | `./scripts/readiness.sh` |
| Guarded reset | `./scripts/reset.sh --confirm dbx_agentic_dev` |

Evidence labels: local, remote dry-run, deployed, live runtime, or replay. Never substitute one for another.
The local fallback performs a clean full replay because the open-source CLI catalog is process-local; only the
Free Edition command demonstrates an incremental Lakeflow update.
