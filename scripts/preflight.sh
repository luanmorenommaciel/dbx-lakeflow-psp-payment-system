#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/java_home.sh
source "$(dirname "$0")/java_home.sh"

mode="${1:---config}"
if [[ "$mode" != "--config" && "$mode" != "--runtime" ]]; then
  echo "PREFLIGHT=BLOCKED reason=unknown_mode usage='$0 [--config|--runtime]'"
  exit 2
fi

profile="${DATABRICKS_CONFIG_PROFILE:-}"
if [[ -z "$profile" ]]; then
  echo "PREFLIGHT=BLOCKED reason=DATABRICKS_CONFIG_PROFILE_missing"
  exit 1
fi

required_cli="1.13.0"
actual_cli="$(databricks version | awk '{print $3}' | sed 's/^v//')"
if [[ "$actual_cli" != "$required_cli" ]]; then
  echo "PREFLIGHT=BLOCKED reason=databricks_cli expected=$required_cli actual=$actual_cli"
  exit 1
fi

if ! java_home="$(resolve_workshop_java_home)"; then
  echo "PREFLIGHT=BLOCKED reason=java_17_or_21_missing"
  exit 1
fi

uv --version
git --version
python3 --version
"$java_home/bin/java" -version 2>&1 | head -1
databricks current-user me -p "$profile" >/dev/null
if [[ "$mode" == "--runtime" ]]; then
  if ! databricks experimental aitools tools query \
    -p "$profile" -o json "SELECT 1 AS workshop_runtime_ready" >/dev/null; then
    echo "PREFLIGHT=BLOCKED reason=serverless_compute_unavailable profile=$profile"
    exit 1
  fi
  if [[ -f databricks.yml ]]; then
    databricks bundle validate --strict -t dev -p "$profile"
  else
    echo "PREFLIGHT_RUNTIME_NOTICE=bundle_not_built_yet"
  fi
  echo "PREFLIGHT_RUNTIME=READY profile=$profile"
fi
echo "PREFLIGHT=READY profile=$profile"
