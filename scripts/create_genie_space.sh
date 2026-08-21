#!/usr/bin/env bash
set -euo pipefail

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
catalog="${WORKSHOP_CATALOG:-${BUNDLE_VAR_catalog:-workspace}}"
schema="${WORKSHOP_SCHEMA:-${BUNDLE_VAR_schema:-dbx_agentic_dev}}"
evidence_dir=".workshop-evidence/remote"
mkdir -p "$evidence_dir"

warehouses_json="$(databricks warehouses list -p "$profile" -o json)"
warehouse_id="${DATABRICKS_WAREHOUSE_ID:-}"
if [[ -z "$warehouse_id" ]]; then
  warehouse_id="$(WAREHOUSES_JSON="$warehouses_json" uv run python - <<'PY'
import json, os
payload = json.loads(os.environ["WAREHOUSES_JSON"])
items = payload if isinstance(payload, list) else payload.get("warehouses", [])
running = [item for item in items if item.get("state") == "RUNNING"]
selected = (running or items)
if not selected:
    raise SystemExit("GENIE_SPACE=BLOCKED reason=no_sql_warehouse")
print(selected[0]["id"])
PY
)"
fi

user_json="$(databricks current-user me -p "$profile" -o json)"
user_name="$(USER_JSON="$user_json" uv run python - <<'PY'
import json, os
payload = json.loads(os.environ["USER_JSON"])
print(payload.get("userName") or payload.get("user_name"))
PY
)"
parent_path="/Workspace/Users/$user_name"

serialized="$(CATALOG="$catalog" SCHEMA="$schema" uv run python - <<'PY'
import json, os
from pathlib import Path
payload = Path("docs/genie/space.json").read_text()
payload = payload.replace("__CATALOG__", os.environ["CATALOG"])
payload = payload.replace("__SCHEMA__", os.environ["SCHEMA"])
print(json.dumps(json.loads(payload), separators=(",", ":")))
PY
)"

if [[ -n "${DATABRICKS_GENIE_SPACE_ID:-}" ]]; then
  result="$(databricks genie update-space "$DATABRICKS_GENIE_SPACE_ID" \
    --warehouse-id "$warehouse_id" \
    --serialized-space "$serialized" \
    --title "DBX Agentic PSP Incident" \
    --description "Deterministic payment-quality investigation for the four-hour workshop" \
    --parent-path "$parent_path" \
    -p "$profile" -o json)"
  space_id="$DATABRICKS_GENIE_SPACE_ID"
  action="updated"
else
  result="$(databricks genie create-space "$warehouse_id" "$serialized" \
    --title "DBX Agentic PSP Incident" \
    --description "Deterministic payment-quality investigation for the four-hour workshop" \
    --parent-path "$parent_path" \
    -p "$profile" -o json)"
  space_id="$(RESULT_JSON="$result" uv run python - <<'PY'
import json, os
payload = json.loads(os.environ["RESULT_JSON"])
print(payload.get("space_id") or payload.get("spaceId") or payload.get("id"))
PY
)"
  action="created"
fi

if [[ -z "$space_id" || "$space_id" == "None" ]]; then
  echo "GENIE_SPACE=FAIL reason=missing_space_id"
  exit 1
fi
printf '%s\n' "$result" > "$evidence_dir/genie-space.json"
printf '%s\n' "$space_id" > "$evidence_dir/genie-space-id.txt"
printf '%s\n' "$warehouse_id" > "$evidence_dir/genie-warehouse-id.txt"
echo "GENIE_SPACE=READY action=$action id=$space_id warehouse=$warehouse_id"
