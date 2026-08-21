#!/usr/bin/env bash
set -euo pipefail

catalog="${WORKSHOP_CATALOG:-${BUNDLE_VAR_catalog:-workspace}}"
schema="${WORKSHOP_SCHEMA:-${BUNDLE_VAR_schema:-dbx_agentic_dev}}"
if [[ "${1:-}" != "--confirm" || "${2:-}" != "$schema" ]]; then
  echo "RESET=REFUSED usage='$0 --confirm $schema'"
  exit 2
fi

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
databricks bundle destroy -t dev -p "$profile" --auto-approve
databricks experimental aitools tools query \
  -p "$profile" \
  --warehouse "${DATABRICKS_WAREHOUSE_ID:?Set DATABRICKS_WAREHOUSE_ID}" \
  "DROP SCHEMA IF EXISTS ${catalog}.${schema} CASCADE" >/dev/null
echo "RESET=COMPLETE catalog=$catalog schema=$schema"
