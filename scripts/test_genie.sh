#!/usr/bin/env bash
set -euo pipefail

profile="${DATABRICKS_CONFIG_PROFILE:?Set DATABRICKS_CONFIG_PROFILE}"
space_id="${DATABRICKS_GENIE_SPACE_ID:-}"
if [[ -z "$space_id" && -f .workshop-evidence/remote/genie-space-id.txt ]]; then
  space_id="$(<.workshop-evidence/remote/genie-space-id.txt)"
fi
if [[ -z "$space_id" ]]; then
  echo "GENIE_TEST=BLOCKED reason=space_id_missing"
  exit 1
fi

evidence_dir=".workshop-evidence/remote/genie"
mkdir -p "$evidence_dir"
mapfile_cmd="$(command -v mapfile || true)"
if [[ -n "$mapfile_cmd" ]]; then
  mapfile -t prompts < <(uv run python - <<'PY'
import yaml
from pathlib import Path
for item in yaml.safe_load(Path("docs/genie/questions.yaml").read_text())["questions"]:
    print(item["prompt"])
PY
)
else
  prompts=()
  while IFS= read -r prompt; do prompts+=("$prompt"); done < <(uv run python - <<'PY'
import yaml
from pathlib import Path
for item in yaml.safe_load(Path("docs/genie/questions.yaml").read_text())["questions"]:
    print(item["prompt"])
PY
)
fi

index=0
for prompt in "${prompts[@]}"; do
  index=$((index + 1))
  databricks genie start-conversation "$space_id" "$prompt" \
    --timeout 10m -p "$profile" -o json > "$evidence_dir/question-$index.json"
  echo "GENIE_QUESTION=PASS index=$index"
done
echo "GENIE_TEST=PASS questions=$index space_id=$space_id evidence=$evidence_dir"
