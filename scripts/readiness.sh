#!/usr/bin/env bash
set -euo pipefail

local_receipt=".workshop-evidence/local/completion.env"
remote_receipt=".workshop-evidence/remote/completion.env"
expected_tasks=9

local_state="MISSING"
remote_state="MISSING"
restore_state="MISSING"
if [[ -f "$local_receipt" ]] && grep -q '^state=PASS$' "$local_receipt"; then
  local_state="PASS"
fi
if [[ -f "$remote_receipt" ]] && grep -q '^state=PASS$' "$remote_receipt"; then
  remote_state="PASS"
fi
if [[ -f "$remote_receipt" ]] && grep -q '^reset_and_restore=true$' "$remote_receipt"; then
  restore_state="PASS"
fi

archived_tasks="$(find tasks/done -maxdepth 1 -name 'T-*.md' -type f 2>/dev/null | wc -l | tr -d ' ')"
claude_state="PASS"
for legacy in .claude/agents .claude/commands .claude/kb .claude/sdd .claude/settings.json .claude/settings.local.json; do
  if [[ -e "$legacy" ]]; then
    claude_state="FAIL"
  fi
done

echo "READINESS_CHECK local=$local_state remote=$remote_state reset_restore=$restore_state tasks_done=$archived_tasks/$expected_tasks claude=$claude_state"
if [[ "$local_state" == "PASS" \
  && "$remote_state" == "PASS" \
  && "$restore_state" == "PASS" \
  && "$archived_tasks" == "$expected_tasks" \
  && "$claude_state" == "PASS" ]]; then
  echo "WORKSHOP_READINESS=READY"
  exit 0
fi

echo "WORKSHOP_READINESS=NOT_READY"
exit 1
