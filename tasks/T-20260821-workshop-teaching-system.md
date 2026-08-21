---
id: T-20260821-workshop-teaching-system
title: "Build the learner, instructor, and fallback system"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: technical-writer
parent: (none)
depends_on: [T-20260821-workshop-contracts, T-20260821-workshop-genie-evidence]
supersedes: (none)
touches_paths: [readme.md, data/fallback/]
creates_paths: [docs/learner/, docs/instructor/, docs/fallback/]
source_note: "four-hour agenda and four landing-page fallbacks"
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

# Build the learner, instructor, and fallback system

> **Why:** The reference implementation lacks resettable learner checkpoints, recovery paths, and delivery instructions.

## Goal

Produce a timed four-hour workshop, instructor cues, checkpoints, exact commands, evidence expectations, and rehearsed fallbacks.

## Context

Materials label every proof as local, remote dry-run, deployed, live runtime, or replay.

## Behavior

- **B-1** — GIVEN a second instructor and a fresh learner clone WHEN they follow the runbook THEN they can choose the correct live or fallback branch at each timed gate

## Success Criteria

```bash
# eval_1: materials and fallback contracts are internally complete
eval_1() {
  uv run pytest -q tests -k 'materials or fallback'
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "materials and fallback contracts are internally complete"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
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
  required_tools: [git, bash, pytest]
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

Remove workshop materials and fallback assets without touching the reference implementation.

## Observability Hooks

Link every checkpoint to a command, expected output, timebox, and recovery branch.

## Anti-Patterns

- Add optional production topics to the core four-hour path.
- Hide waiting time or recovery decisions.
- Present replay as a live successful operation.

## Do-Not-Touch

- `pipelines/`
- `gen/`
- `data/`
- `presentation/`

## Open Questions

(none — this task is fully specified)
