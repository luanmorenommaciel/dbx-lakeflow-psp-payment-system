#!/usr/bin/env bash
set -uo pipefail

module="${1:-}"
if [[ ! "$module" =~ ^0[1-6]$ ]]; then
  echo "CHECKPOINT=RECOVERY_REQUIRED reason=usage usage='$0 01|02|03|04|05|06'"
  exit 2
fi

run() {
  echo "CHECKPOINT_STEP module=$module command=$*"
  "$@"
}

if ! run uv run pytest -q "tests/checkpoints/test_progressive_modules.py" -k "module_${module}"; then
  echo "CHECKPOINT=FAIL module=$module phase=artifact_contract"
  exit 1
fi

case "$module" in
  01)
    run uv run pytest -q tests/contract || {
      echo "CHECKPOINT=FAIL module=$module phase=contract_tests"
      exit 1
    }
    ;;
  02)
    run uv run pytest -q tests/generator || {
      echo "CHECKPOINT=FAIL module=$module phase=generator_tests"
      exit 1
    }
    ;;
  03)
    run ./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml || {
      echo "CHECKPOINT=FAIL module=$module phase=sdp_graph"
      exit 1
    }
    ;;
  04)
    run uv run pytest -q tests/contract tests/checkpoints/test_progressive_modules.py \
      -k "module_01 or module_02 or module_03 or module_04" || {
      echo "CHECKPOINT=FAIL module=$module phase=quality_contract"
      exit 1
    }
    run ./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml || {
      echo "CHECKPOINT=FAIL module=$module phase=sdp_graph"
      exit 1
    }
    ;;
  05)
    run ./scripts/e2e_local.sh --module05 || {
      echo "CHECKPOINT=FAIL module=$module phase=local_story"
      exit 1
    }
    ;;
  06)
    run uv run pytest -q tests/landing_promises tests/test_materials.py || {
      echo "CHECKPOINT=FAIL module=$module phase=delivery_contract"
      exit 1
    }
    ;;
esac

echo "CHECKPOINT=PASS module=$module"
