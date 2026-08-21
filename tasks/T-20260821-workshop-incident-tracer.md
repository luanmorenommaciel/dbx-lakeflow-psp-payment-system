---
id: T-20260821-workshop-incident-tracer
title: "Deliver the DQX quarantine and replay tracer bullet"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: data-quality-engineer
parent: (none)
depends_on: [T-20260821-workshop-baseline-tracer]
supersedes: (none)
touches_paths: [pipelines/src/psp-agentic/silver.py, pipelines/src/psp-agentic/gold.py, tests/pipeline/]
creates_paths: [configs/dqx-rules.yaml, scripts/release_incident.sh]
source_note: "landing promise for DQX, quarantine, and eight incidents"
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

# Deliver the DQX quarantine and replay tracer bullet

> **Why:** The existing implementation silently drops failed expectations, while the workshop promises explainable quarantine and replay.

## Goal

Apply one DQX rule source, route seven invalid conditions to quarantine, and let the late valid chargeback update merchant risk.

## Context

Do not duplicate domain rules as Lakeflow expectations.

## Behavior

- **B-1** — GIVEN batch-002-incident is released WHEN DQX checks execute THEN seven invalid conditions are preserved in quarantine with explainable metadata
- **B-2** — GIVEN the held late chargeback is released WHEN the incremental pipeline reruns THEN it passes Silver and changes the named merchant risk result deterministically

## Success Criteria

```bash
# eval_1: incident routing and replay assertions pass
eval_1() {
  uv run pytest -q tests/pipeline -k incident
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "incident routing and replay assertions pass"
    runnable: bash
    check_type: deterministic
    verifies: [B-1, B-2]
    terminal: true
    expected_duration_sec: 300
retry_policy:
  max_iterations: 15
  circuit_breaker_no_progress: 3
  on_terminal_failure: park_with_context
agent_contract:
  version: 2
  read: [intent, behavior, contract, guardrails]
  produce: [code, tests]
  required_tools: [uv, spark-pipelines, pytest]
  timeout_minutes: 30
  sandbox_type: host
  output_artifacts: []
  mcp_dependencies: []
  emit: [pass, fail, retry_with_reason, parked_with_context]
  backend_metadata: {}
```

## Exit Check

```bash
eval_1
```

## Rollback Plan

Remove incident-specific rules and replay release while leaving baseline tracer intact.

## Observability Hooks

Record DQX summary metrics, quarantine reason counts, and Gold before/after values.

## Anti-Patterns

- Use ON VIOLATION DROP ROW.
- Duplicate DQX rules in decorators or SQL constraints.
- Mix the valid late event into quarantine.

## Do-Not-Touch

- `pipelines/`
- `gen/`
- `data/`

## Open Questions

(none — this task is fully specified)
