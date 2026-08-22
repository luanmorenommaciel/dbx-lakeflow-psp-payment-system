#!/usr/bin/env bash
set -euo pipefail

tags=(
  workshop-v1-starter
  workshop-v1-m01-contract-plan
  workshop-v1-m02-synthetic-data
  workshop-v1-m03-bronze
  workshop-v1-m04-dqx-silver
  workshop-v1-m05-gold-dabs
  workshop-v1-m06-genie-delivered
  workshop-v1-solution
)

for tag in "${tags[@]}"; do
  if [[ "$(git cat-file -t "$tag" 2>/dev/null || true)" != "tag" ]]; then
    echo "CHECKPOINT_SERIES=FAIL reason=missing_or_lightweight_tag tag=$tag"
    exit 1
  fi
done

scratch="$(mktemp -d /tmp/dbx-workshop-tags.XXXXXX)"
cleanup() {
  for path in "$scratch"/*; do
    if [[ -d "$path" ]]; then
      git worktree remove --force "$path" >/dev/null 2>&1 || true
    fi
  done
  rmdir "$scratch" 2>/dev/null || true
}
trap cleanup EXIT

verify_tag() {
  local tag="$1"
  local module="$2"
  local path="$scratch/$module"
  git worktree add --detach "$path" "$tag" >/dev/null
  (
    cd "$path"
    UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$(git rev-parse --show-toplevel)/.venv}" \
      ./scripts/checkpoint.sh "$module"
  )
  git worktree remove --force "$path" >/dev/null
}

starter="$scratch/starter"
git worktree add --detach "$starter" workshop-v1-starter >/dev/null
for product in \
  "$starter/configs/contracts/psp-payment.contract.yaml" \
  "$starter/configs/contracts/incident-ledger.yaml" \
  "$starter/configs/dqx-rules.yaml" \
  "$starter/gen/synthetic/generator.py" \
  "$starter/pipelines/src/bronze.py" \
  "$starter/databricks.yml" \
  "$starter/docs/genie/questions.yaml"; do
  if [[ -e "$product" ]]; then
    echo "CHECKPOINT_SERIES=FAIL reason=starter_contains_solution path=${product#"$starter/"}"
    exit 1
  fi
done
for harness in \
  "$starter/AGENTS.md" \
  "$starter/CLAUDE.md" \
  "$starter/docs/specs/psp-payment.md" \
  "$starter/docs/learner/prompts/01-contract-plan.md" \
  "$starter/scripts/checkpoint.sh" \
  "$starter/data/fallback/manifest.json"; do
  if [[ ! -e "$harness" ]]; then
    echo "CHECKPOINT_SERIES=FAIL reason=starter_missing_harness path=${harness#"$starter/"}"
    exit 1
  fi
done
git worktree remove --force "$starter" >/dev/null

verify_tag workshop-v1-m01-contract-plan 01
verify_tag workshop-v1-m02-synthetic-data 02
verify_tag workshop-v1-m03-bronze 03
verify_tag workshop-v1-m04-dqx-silver 04
verify_tag workshop-v1-m05-gold-dabs 05
verify_tag workshop-v1-m06-genie-delivered 06

mkdir -p .workshop-evidence/rehearsals
printf '%s\n' \
  "contract=DBXWorkshopCheckpointSeries/v1" \
  "state=PASS" \
  "annotated_tags=8" \
  "modules=6" \
  > .workshop-evidence/rehearsals/checkpoints.env
echo "CHECKPOINT_SERIES=PASS annotated_tags=8 modules=6"
