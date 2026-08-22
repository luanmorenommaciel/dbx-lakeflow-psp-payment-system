#!/usr/bin/env bash
set -euo pipefail

local_receipt=".workshop-evidence/local/completion.env"
remote_receipt=".workshop-evidence/remote/completion.env"
checkpoint_receipt=".workshop-evidence/rehearsals/checkpoints.env"
claude_receipt=".workshop-evidence/rehearsals/claude.env"
codex_receipt=".workshop-evidence/rehearsals/codex.env"
fallback_receipt=".workshop-evidence/rehearsals/fallback.env"
website_receipt=".workshop-evidence/rehearsals/website.env"

local_state="MISSING"
remote_state="MISSING"
restore_state="MISSING"
checkpoint_state="MISSING"
claude_rehearsal="MISSING"
codex_rehearsal="MISSING"
fallback_state="MISSING"
website_state="MISSING"
if [[ -f "$local_receipt" ]] && grep -q '^state=PASS$' "$local_receipt"; then
  local_state="PASS"
fi
if [[ -f "$remote_receipt" ]] && grep -q '^state=PASS$' "$remote_receipt"; then
  remote_state="PASS"
fi
if [[ -f "$remote_receipt" ]] && grep -q '^reset_and_restore=true$' "$remote_receipt"; then
  restore_state="PASS"
fi
if [[ -f "$checkpoint_receipt" ]] && grep -q '^state=PASS$' "$checkpoint_receipt"; then
  checkpoint_state="PASS"
fi
if [[ -f "$claude_receipt" ]] && grep -q '^state=PASS$' "$claude_receipt"; then
  claude_rehearsal="PASS"
fi
if [[ -f "$codex_receipt" ]] && grep -q '^state=PASS$' "$codex_receipt"; then
  codex_rehearsal="PASS"
fi
if [[ -f "$fallback_receipt" ]] && grep -q '^state=PASS$' "$fallback_receipt"; then
  fallback_state="PASS"
fi
if [[ -f "$website_receipt" ]] && grep -q '^state=PASS$' "$website_receipt"; then
  website_state="PASS"
fi

claude_state="PASS"
for legacy in .claude/agents .claude/commands .claude/kb .claude/sdd .claude/settings.json .claude/settings.local.json; do
  if [[ -e "$legacy" ]]; then
    claude_state="FAIL"
  fi
done

echo "READINESS_CHECK local=$local_state remote=$remote_state reset_restore=$restore_state checkpoints=$checkpoint_state claude_rehearsal=$claude_rehearsal codex_rehearsal=$codex_rehearsal fallback=$fallback_state website=$website_state claude_contract=$claude_state"
if [[ "$local_state" == "PASS" \
  && "$remote_state" == "PASS" \
  && "$restore_state" == "PASS" \
  && "$checkpoint_state" == "PASS" \
  && "$claude_rehearsal" == "PASS" \
  && "$codex_rehearsal" == "PASS" \
  && "$fallback_state" == "PASS" \
  && "$website_state" == "PASS" \
  && "$claude_state" == "PASS" ]]; then
  echo "WORKSHOP_READINESS=READY"
  exit 0
fi

echo "WORKSHOP_READINESS=NOT_READY"
exit 1
