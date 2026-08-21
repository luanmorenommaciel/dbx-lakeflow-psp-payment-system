#!/usr/bin/env bash
set -euo pipefail

./scripts/preflight.sh
./scripts/install_agent_skills.sh
uv sync --frozen
echo "BOOTSTRAP=READY"
