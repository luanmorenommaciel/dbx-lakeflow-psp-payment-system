---
id: T-20260821-workshop-genie-evidence
title: "Make the merchant incident explainable with UC and Genie"
status: ready
format_version: 3
profile: standard
effort: M
budget_iterations: 15
agent: analytics-engineer
parent: (none)
depends_on: [T-20260821-workshop-free-bundle]
supersedes: (none)
touches_paths: [pipelines/src/psp-agentic/gold.py, tests/]
creates_paths: [docs/genie/]
source_note: "landing promise for Unity Catalog and Genie Code evidence"
created: "2026-08-21T00:00:00Z"
tags: []
owner: (none)
priority: P2
severity: feature
due_date: (none)
precondition: (none)
blocked_reason: (none)
security_class: (none)
source_action_item: (none)
tracker_ref: (none)
execution_backend: codex
signed_off: false
signed_off_by: (none)
signed_off_at: (none)
accepted: false
accepted_by: (none)
accepted_at: (none)
---

# Make the merchant incident explainable with UC and Genie

> **Why:** A deployed table alone does not prove the promised natural-language investigation experience.

## Goal

Add UC semantics and four rehearsed Genie Code investigations, plus an optional instructor Genie Space path when entitlement is confirmed.

## Context

Genie Code is mandatory and documented in Free Edition; Genie Space is additive until preflight proves availability.

## Behavior

- **B-1** — GIVEN Gold and quarantine tables are populated WHEN the four Genie Code prompts are asked THEN the answers identify the expected merchant and explain the risk change using governed data

## Success Criteria

```bash
# eval_1: Genie prompts and expected results cover the landing promise
eval_1() {
  uv run pytest -q tests -k genie_contract
}

# eval_2: all four questions complete against the configured live Genie Space
eval_2() {
  ./scripts/test_genie.sh
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "Genie prompts and expected results cover the landing promise"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 60
  - id: eval_2
    description: "all four questions complete against the configured live Genie Space"
    runnable: bash
    check_type: live
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 600
retry_policy:
  max_iterations: 15
  circuit_breaker_no_progress: 3
  on_terminal_failure: park_with_context
agent_contract:
  version: 2
  read: [intent, behavior, contract, guardrails]
  produce: [code, tests]
  required_tools: [databricks, pytest]
  timeout_minutes: 30
  sandbox_type: host
  output_artifacts: []
  mcp_dependencies: []
  emit: [pass, fail, retry_with_reason, parked_with_context]
  backend_metadata: {}
```

## Exit Check

```bash
eval_1 && eval_2
```

## Rollback Plan

Remove only workshop Genie configuration and semantic additions.

## Observability Hooks

Store prompts, generated SQL, bounded result digests, and screenshots with no personal identifiers.

## Anti-Patterns

- Claim a Genie result from static SQL alone.
- Make learner success depend on unverified Genie Space entitlement.

## Do-Not-Touch

- `pipelines/`
- `gen/`
- `data/`

## Open Questions

(none — this task is fully specified)
