#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${1:-}" != "--confirm-remote" ]]; then
  echo "REMOTE_E2E=REFUSED usage='$0 --confirm-remote [--reset-and-restore]'"
  exit 2
fi
cleanup_mode="${2:-}"
if [[ -n "$cleanup_mode" && "$cleanup_mode" != "--reset-and-restore" ]]; then
  echo "REMOTE_E2E=REFUSED reason=unknown_option option=$cleanup_mode"
  exit 2
fi

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
evidence_dir=".workshop-evidence/remote"
mkdir -p "$evidence_dir"
stage="initializing"

stale_evidence=(
  "$evidence_dir/01-preflight.log"
  "$evidence_dir/02-bundle-validate.json"
  "$evidence_dir/02-pipeline-dry-run.json"
  "$evidence_dir/03-bundle-deploy.log"
  "$evidence_dir/04-pipeline-dry-run.json"
  "$evidence_dir/05-baseline-upload.log"
  "$evidence_dir/06-baseline-run.log"
  "$evidence_dir/07-baseline.log"
  "$evidence_dir/08-genie-create.log"
  "$evidence_dir/09-genie-baseline.log"
  "$evidence_dir/10-incident-replay.log"
  "$evidence_dir/11-replay.log"
  "$evidence_dir/12-genie-replay.log"
  "$evidence_dir/13-drift-upload.log"
  "$evidence_dir/14-drift-run.log"
  "$evidence_dir/15-drift-assert.log"
  "$evidence_dir/16-reset.log"
  "$evidence_dir/17-restore-deploy.log"
  "$evidence_dir/18-restore-upload.log"
  "$evidence_dir/19-restore-baseline.log"
  "$evidence_dir/20-restore-assert.log"
  "$evidence_dir/21-restore-genie.log"
  "$evidence_dir/22-restore-genie-test.log"
  "$evidence_dir/completion.env"
  "$evidence_dir/failure.env"
  "$evidence_dir/failure-cleanup.log"
)
rm -f "${stale_evidence[@]}"

cleanup_after_failure() {
  failure_rc=$?
  trap - ERR
  printf '%s\n' \
    "contract=DBXWorkshopRemoteFailure/v1" \
    "state=FAIL" \
    "stage=$stage" \
    > "$evidence_dir/failure.env"
  if [[ "$cleanup_mode" == "--reset-and-restore" ]]; then
    {
      if [[ "$stage" != "preflight" && "$stage" != "validation" ]]; then
        databricks bundle destroy -t dev -p "$profile" --auto-approve || true
      fi
      bundle_root="$(
        databricks bundle summary -t dev -p "$profile" -o json 2>/dev/null \
          | jq -r '.workspace.root_path // empty' \
          || true
      )"
      if [[ "$bundle_root" == /Workspace/Users/*/.bundle/dbx-agentic-psp-workshop/dev ]]; then
        databricks workspace delete "$bundle_root" --recursive -p "$profile" || true
      fi
    } > "$evidence_dir/failure-cleanup.log" 2>&1
  fi
  echo "REMOTE_E2E=FAIL stage=$stage evidence=$evidence_dir"
  exit "$failure_rc"
}
trap cleanup_after_failure ERR

stage="preflight"
./scripts/preflight.sh --runtime | tee "$evidence_dir/01-preflight.log"
stage="validation"
databricks bundle validate --strict -t dev -p "$profile" -o json \
  | tee "$evidence_dir/02-bundle-validate.json"
stage="deploy"
databricks bundle deploy -t dev -p "$profile" \
  | tee "$evidence_dir/03-bundle-deploy.log"
stage="pipeline-dry-run"
databricks pipelines dry-run workshop_pipeline -t dev -p "$profile" -o json \
  | tee "$evidence_dir/04-pipeline-dry-run.json"
stage="baseline-upload"
./scripts/upload_fallback.sh baseline | tee "$evidence_dir/05-baseline-upload.log"
stage="baseline-run"
databricks bundle run workshop_pipeline -t dev -p "$profile" --full-refresh-all \
  | tee "$evidence_dir/06-baseline-run.log"
stage="baseline-assert"
./scripts/verify_remote_data.sh baseline | tee "$evidence_dir/07-baseline.log"
stage="genie-create"
./scripts/create_genie_space.sh | tee "$evidence_dir/08-genie-create.log"
stage="genie-baseline"
./scripts/test_genie.sh | tee "$evidence_dir/09-genie-baseline.log"
stage="incident-replay"
./scripts/release_incident.sh --remote | tee "$evidence_dir/10-incident-replay.log"
stage="replay-assert"
./scripts/verify_remote_data.sh replay | tee "$evidence_dir/11-replay.log"
stage="genie-replay"
./scripts/test_genie.sh | tee "$evidence_dir/12-genie-replay.log"

if [[ "$cleanup_mode" == "--reset-and-restore" ]]; then
  stage="drift-upload"
  ./scripts/upload_fallback.sh drift | tee "$evidence_dir/13-drift-upload.log"
  stage="drift-run"
  databricks bundle run workshop_pipeline -t dev -p "$profile" \
    | tee "$evidence_dir/14-drift-run.log"
  stage="drift-assert"
  ./scripts/verify_remote_drift.sh | tee "$evidence_dir/15-drift-assert.log"
  stage="reset"
  export DATABRICKS_WAREHOUSE_ID="${DATABRICKS_WAREHOUSE_ID:-$(<"$evidence_dir/genie-warehouse-id.txt")}"
  ./scripts/reset.sh --confirm "${WORKSHOP_SCHEMA:-${BUNDLE_VAR_schema:-dbx_agentic_dev}}" \
    | tee "$evidence_dir/16-reset.log"
  stage="restore-deploy"
  databricks bundle deploy -t dev -p "$profile" \
    | tee "$evidence_dir/17-restore-deploy.log"
  stage="restore-upload"
  ./scripts/upload_fallback.sh baseline | tee "$evidence_dir/18-restore-upload.log"
  stage="restore-run"
  databricks bundle run workshop_pipeline -t dev -p "$profile" --full-refresh-all \
    | tee "$evidence_dir/19-restore-baseline.log"
  stage="restore-assert"
  ./scripts/verify_remote_data.sh baseline | tee "$evidence_dir/20-restore-assert.log"
  DATABRICKS_GENIE_SPACE_ID="$(<"$evidence_dir/genie-space-id.txt")"
  export DATABRICKS_GENIE_SPACE_ID
  stage="restore-genie"
  ./scripts/create_genie_space.sh | tee "$evidence_dir/21-restore-genie.log"
  stage="restore-genie-test"
  ./scripts/test_genie.sh | tee "$evidence_dir/22-restore-genie-test.log"
  echo "REMOTE_RESTORE=PASS state=baseline"
fi

stage="complete"
rm -f "$evidence_dir/failure.env" "$evidence_dir/failure-cleanup.log"
printf '%s\n' \
  "contract=DBXWorkshopRemoteCompletion/v1" \
  "state=PASS" \
  "reset_and_restore=$([[ "$cleanup_mode" == "--reset-and-restore" ]] && echo true || echo false)" \
  > "$evidence_dir/completion.env"
echo "REMOTE_E2E=PASS profile=$profile evidence=$evidence_dir"
