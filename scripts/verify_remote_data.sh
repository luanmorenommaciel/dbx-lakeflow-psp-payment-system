#!/usr/bin/env bash
set -euo pipefail

phase="${1:-}"
if [[ "$phase" != "baseline" && "$phase" != "replay" ]]; then
  echo "Usage: $0 baseline|replay"
  exit 2
fi

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
catalog="${WORKSHOP_CATALOG:-${BUNDLE_VAR_catalog:-workspace}}"
schema="${WORKSHOP_SCHEMA:-${BUNDLE_VAR_schema:-dbx_agentic_dev}}"
expected_chargebacks=0
expected_score=25
expected_band="normal"
if [[ "$phase" == "replay" ]]; then
  expected_chargebacks=1
  expected_score=45
  expected_band="elevated"
fi

sql="SELECT CASE WHEN quarantine_count = 1209 AND brl_rejected_count = 1204 AND quality_risk_score = 25 AND chargeback_count = ${expected_chargebacks} AND chargeback_risk_points = $((expected_chargebacks * 20)) AND risk_score = ${expected_score} AND risk_band = '${expected_band}' THEN 'PASS' ELSE 'FAIL' END AS verification, merchant_id, quarantine_count, brl_rejected_count, chargeback_count, risk_score, risk_band FROM ${catalog}.${schema}.gold_merchant_risk WHERE merchant_id = 'm-007'"
evidence_dir=".workshop-evidence/remote"
mkdir -p "$evidence_dir"
output_file="$evidence_dir/data-$phase.json"
databricks experimental aitools tools query -p "$profile" -o json "$sql" > "$output_file"
if ! uv run python - "$output_file" <<'PY'
import json, sys
payload = json.load(open(sys.argv[1]))
text = json.dumps(payload)
if 'PASS' not in text:
    raise SystemExit(1)
PY
then
  echo "REMOTE_DATA=FAIL phase=$phase evidence=$output_file"
  exit 1
fi
echo "REMOTE_DATA=PASS phase=$phase evidence=$output_file"
