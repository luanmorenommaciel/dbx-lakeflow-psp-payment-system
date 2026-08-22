#!/usr/bin/env bash
set -euo pipefail

mode="${1:---remote}"
if [[ "$mode" == "--local" ]]; then
  # shellcheck source=scripts/java_home.sh
  source "$(dirname "$0")/java_home.sh"
  if ! java_home="$(resolve_workshop_java_home)"; then
    echo "INCIDENT_REPLAY=BLOCKED reason=java_17_or_21_missing"
    exit 1
  fi
  unset SPARK_HOME
  JAVA_HOME="$java_home" PATH="$java_home/bin:$PATH" \
    uv run python -m gen.synthetic.release --output data/fallback/generated
  backup_root="$(mktemp -d /tmp/dbx-psp-replay.XXXXXX)"
  if [[ -d spark-warehouse ]]; then
    mv spark-warehouse "$backup_root/spark-warehouse"
  fi
  if [[ -d /tmp/dbx-psp-sdp ]]; then
    mv /tmp/dbx-psp-sdp "$backup_root/pipeline-state"
  fi
  ./scripts/sdp.sh run --spec pipelines/spark-pipeline.yaml --full-refresh-all
  echo "INCIDENT_REPLAY=COMPLETE mode=local proof=full_replay baseline_backup=$backup_root"
  exit 0
fi

if [[ "$mode" != "--remote" ]]; then
  echo "Usage: $0 [--local|--remote]"
  exit 2
fi

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
./scripts/upload_fallback.sh replay
databricks bundle run workshop_pipeline -t dev -p "$profile" --refresh-all
echo "INCIDENT_REPLAY=COMPLETE mode=remote"
