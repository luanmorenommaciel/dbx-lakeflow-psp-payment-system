#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=scripts/java_home.sh
source "$(dirname "$0")/java_home.sh"
if ! java_home="$(resolve_workshop_java_home)"; then
  echo "SDP=BLOCKED reason=java_17_or_21_missing"
  exit 1
fi

# A globally installed Spark can shadow the pinned PySpark 4.2 environment.
unset SPARK_HOME
export JAVA_HOME="$java_home"
export PATH="$java_home/bin:$PATH"

exec uv run spark-pipelines "$@"
