#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/java_home.sh
source "$(dirname "$0")/java_home.sh"
if ! java_home="$(resolve_workshop_java_home)"; then
  echo "FALLBACK=BLOCKED reason=java_17_or_21_missing"
  exit 1
fi

unset SPARK_HOME
JAVA_HOME="$java_home" PATH="$java_home/bin:$PATH" \
  uv run python -m gen.synthetic.cli \
    --output data/fallback/generated \
    --seed 22082026 \
    --manifest-out data/fallback/manifest.json

echo "FALLBACK=GENERATED"
