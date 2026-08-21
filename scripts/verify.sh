#!/usr/bin/env bash
set -euo pipefail

mode="${1:---local}"
uv run pytest -q
uv run ruff check gen tests pipelines/src/psp-agentic
./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml

if [[ "$mode" == "--remote" ]]; then
  profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
  databricks pipelines dry-run workshop_pipeline -t dev -p "$profile"
  databricks bundle validate --strict -t dev -p "$profile"
fi

echo "VERIFY=PASS mode=$mode"
