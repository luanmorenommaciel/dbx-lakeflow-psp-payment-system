---
id: T-20260821-workshop-free-bundle
title: "Package the Free Edition dev deployment"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: platform-engineer
parent: (none)
depends_on: [T-20260821-workshop-incident-tracer]
supersedes: (none)
touches_paths: [toolchain.lock.yaml, tests/, scripts/]
creates_paths: [databricks.yml, pipelines/resources/agentic/]
source_note: "Free Edition constraints and landing promise for DAB dev deploy/run"
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

# Package the Free Edition dev deployment

> **Why:** The current bundle is Azure-specific, includes production assumptions, and would collide in a learner room.

## Goal

Create a dev-only serverless DAB with a stable learner-workspace schema, managed Volume, one pipeline, explicit CLI upload/run steps, preflight, bootstrap, and scoped reset.

## Context

The workspace host and profile remain external; default catalog selection is verified by preflight.

## Behavior

- **B-1** — GIVEN an authenticated Free Edition profile WHEN the dev bundle is validated and deployed THEN only the learner's schema, Volume, and serverless pipeline are created
- **B-2** — GIVEN the learner chooses reset WHEN reset executes THEN only exact bundle resources and the learner schema are removed

## Success Criteria

```bash
# eval_1: dev bundle validates strictly
eval_1() {
  databricks bundle validate --strict -t dev -p "$DATABRICKS_CONFIG_PROFILE"
}

# eval_2: bundle policy forbids production resources and broad cleanup
eval_2() {
  uv run pytest -q tests -k bundle_policy
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "dev bundle validates strictly"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 120
  - id: eval_2
    description: "bundle policy forbids production resources and broad cleanup"
    runnable: bash
    check_type: deterministic
    verifies: [B-1, B-2]
    terminal: true
    expected_duration_sec: 60
retry_policy:
  max_iterations: 15
  circuit_breaker_no_progress: 3
  on_terminal_failure: park_with_context
agent_contract:
  version: 2
  read: [intent, behavior, contract, guardrails]
  produce: [code, tests]
  required_tools: [databricks, uv, pytest]
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

Bundle destroy declared resources, then drop only the exact learner schema after confirmation.

## Observability Hooks

Capture strict validation, deploy summary, CLI upload count, pipeline update ID, object inventory, and cleanup receipt.

## Anti-Patterns

- Hard-code the instructor host or org ID.
- Add a production target, schedule, notifications, service principal, or classic cluster.
- Delete objects outside the normalized learner schema.

## Do-Not-Touch

- `pipelines/`
- `configs/`
- `data/`
- `gen/`

## Open Questions

(none — this task is fully specified)
