#!/usr/bin/env bash
set -euo pipefail

mode="${1:-baseline}"
if [[ "$mode" != "baseline" && "$mode" != "replay" && "$mode" != "drift" ]]; then
  echo "UPLOAD=REFUSED usage='$0 [baseline|replay|drift]'"
  exit 2
fi

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
catalog="${WORKSHOP_CATALOG:-${BUNDLE_VAR_catalog:-workspace}}"
schema="${WORKSHOP_SCHEMA:-${BUNDLE_VAR_schema:-dbx_agentic_dev}}"
volume="${WORKSHOP_VOLUME:-${BUNDLE_VAR_landing_volume:-landing}}"
root="data/fallback/generated"
uploaded=0

upload_parts() {
  source_dir="$1"
  target_entity="$2"
  parts=("$source_dir"/part-*.json.gz)
  if [[ ! -e "${parts[0]}" ]]; then
    echo "UPLOAD=BLOCKED reason=missing_generated_parts source=$source_dir"
    exit 1
  fi
  for part in "${parts[@]}"; do
    databricks fs cp "$part" \
      "dbfs:/Volumes/$catalog/$schema/$volume/$target_entity/$(basename "$part")" \
      --overwrite -p "$profile"
    uploaded=$((uploaded + 1))
  done
}

if [[ "$mode" == "baseline" ]]; then
  for entity in merchants orders transactions disputes; do
    upload_parts "$root/$entity" "$entity"
  done
elif [[ "$mode" == "replay" ]]; then
  upload_parts "$root/held/disputes" disputes
else
  source_dir="data/fallback/drift/transactions"
  target="dbfs:/Volumes/$catalog/$schema/$volume/transactions/schema-drift-rescue.json"
  databricks fs cp "$source_dir/schema-drift-rescue.json" "$target" --overwrite -p "$profile"
  uploaded=1
fi

echo "UPLOAD=PASS mode=$mode files=$uploaded target=/Volumes/$catalog/$schema/$volume"
