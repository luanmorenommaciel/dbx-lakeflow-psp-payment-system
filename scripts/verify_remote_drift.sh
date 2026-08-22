#!/usr/bin/env bash
set -euo pipefail

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
catalog="${WORKSHOP_CATALOG:-${BUNDLE_VAR_catalog:-workspace}}"
schema="${WORKSHOP_SCHEMA:-${BUNDLE_VAR_schema:-dbx_agentic_dev}}"
evidence_dir=".workshop-evidence/remote"
mkdir -p "$evidence_dir"
output_file="$evidence_dir/schema-drift.json"

sql="SELECT CASE WHEN _rescued_data IS NOT NULL THEN 'PASS' ELSE 'FAIL' END AS verification, txn_id, _rescued_data FROM ${catalog}.${schema}.bronze_transactions WHERE txn_id = 'txn-drift-000001'"
databricks experimental aitools tools query -p "$profile" -o json "$sql" > "$output_file"

if ! uv run python - "$output_file" <<'PY'
import json
import sys

payload = json.load(open(sys.argv[1], encoding="utf-8"))
if "PASS" not in json.dumps(payload):
    raise SystemExit(1)
PY
then
  echo "REMOTE_DRIFT=FAIL evidence=$output_file"
  exit 1
fi

echo "REMOTE_DRIFT=PASS evidence=$output_file"
