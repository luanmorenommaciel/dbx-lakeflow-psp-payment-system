#!/usr/bin/env bash
set -euo pipefail

mode="${1:---full}"
if [[ "$mode" != "--full" && "$mode" != "--module05" ]]; then
  echo "LOCAL_E2E=FAIL reason=usage usage='$0 [--full|--module05]'"
  exit 2
fi

evidence_dir=".workshop-evidence/local"
mkdir -p "$evidence_dir"

if [[ "$mode" == "--module05" ]]; then
  {
    uv run pytest -q tests/compatibility tests/contract tests/generator tests/pipeline \
      tests/checkpoints/test_progressive_modules.py -k "not module_06"
    uv run ruff check gen tests pipelines/src/psp-agentic
    ./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml
    echo "VERIFY=PASS mode=--module05"
  } | tee "$evidence_dir/01-verify.log"
else
  ./scripts/verify.sh --local | tee "$evidence_dir/01-verify.log"
fi
./scripts/generate_fallback.sh | tee "$evidence_dir/02-generate.log"

backup_root="$(mktemp -d /tmp/dbx-agentic-baseline.XXXXXX)"
if [[ -d spark-warehouse ]]; then
  mv spark-warehouse "$backup_root/spark-warehouse"
fi
if [[ -d /tmp/dbx-agentic-psp-sdp ]]; then
  mv /tmp/dbx-agentic-psp-sdp "$backup_root/pipeline-state"
fi

./scripts/sdp.sh run --spec pipelines/spark-pipeline.yaml --full-refresh-all \
  | tee "$evidence_dir/03-baseline-pipeline.log"
uv run python scripts/assert_local_results.py --phase baseline \
  | tee "$evidence_dir/04-baseline-assert.log"

./scripts/release_incident.sh --local | tee "$evidence_dir/05-replay-pipeline.log"
uv run python scripts/assert_local_results.py --phase replay \
  | tee "$evidence_dir/06-replay-assert.log"

# Leave the committed fallback shape in its baseline state for learners.
./scripts/generate_fallback.sh | tee "$evidence_dir/07-restore-baseline.log"
restore_backup="$(mktemp -d /tmp/dbx-agentic-post-replay.XXXXXX)"
if [[ -d spark-warehouse ]]; then
  mv spark-warehouse "$restore_backup/spark-warehouse"
fi
if [[ -d /tmp/dbx-agentic-psp-sdp ]]; then
  mv /tmp/dbx-agentic-psp-sdp "$restore_backup/pipeline-state"
fi
./scripts/sdp.sh run --spec pipelines/spark-pipeline.yaml --full-refresh-all \
  | tee "$evidence_dir/08-restore-baseline-pipeline.log"
uv run python scripts/assert_local_results.py --phase baseline \
  | tee "$evidence_dir/09-restore-baseline-assert.log"
printf '%s\n' \
  "contract=DBXWorkshopLocalCompletion/v1" \
  "state=PASS" \
  "baseline_risk=25" \
  "replay_risk=45" \
  > "$evidence_dir/completion.env"
echo "LOCAL_E2E=PASS evidence=$evidence_dir recoverable_previous_state=$backup_root replay_backup=$restore_backup state=baseline"
