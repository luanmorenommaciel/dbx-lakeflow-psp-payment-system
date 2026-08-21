---
id: T-20260821-workshop-release-rehearsal
title: "Rehearse and certify every workshop promise"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: release-engineer
parent: (none)
depends_on: [T-20260821-workshop-teaching-system]
supersedes: (none)
touches_paths: [readme.md, docs/]
creates_paths: [scripts/verify.sh, tests/landing_promises/, docs/rehearsals/]
source_note: "live landing page and workshop-ready definition"
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

# Rehearse and certify every workshop promise

> **Why:** Individual checks do not prove that a fresh learner can complete the advertised journey inside four hours.

## Goal

Run one full-live and one forced-fallback rehearsal from fresh clones and reconcile every landing promise.

## Context

Deployment and cleanup need separate explicit authorization even after this TaskPlan is approved.

## Behavior

- **B-1** — GIVEN two fresh clones and two Free Edition accounts WHEN full-live and forced-fallback rehearsals complete THEN every landing promise has an inspectable pass or explicit gap and total delivery fits four hours

## Success Criteria

```bash
# eval_1: all local and promise gates pass
eval_1() {
  ./scripts/verify.sh --local
}

# eval_2: remote release gate passes in an authorized rehearsal
eval_2() {
  ./scripts/e2e_remote.sh --confirm-remote --reset-and-restore
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "all local and promise gates pass"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 600
  - id: eval_2
    description: "full hosted release, Genie, replay, reset, and baseline restore pass"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 1800
retry_policy:
  max_iterations: 15
  circuit_breaker_no_progress: 3
  on_terminal_failure: park_with_context
agent_contract:
  version: 2
  read: [intent, behavior, contract, guardrails]
  produce: [code, tests]
  required_tools: [databricks, uv, spark-pipelines, pytest, git]
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

Destroy only resources identified in rehearsal receipts and preserve sanitized evidence.

## Observability Hooks

Record clone SHA, toolchain digest, run IDs, object inventory, durations, promise matrix, and cleanup state.

## Anti-Patterns

- Rehearse only from a warm developer checkout.
- Omit cleanup verification.
- Mark a fallback replay as live workspace proof.

## Do-Not-Touch

- `pipelines/`
- `gen/`
- `data/`
- `presentation/`

## Open Questions

(none — this task is fully specified)
