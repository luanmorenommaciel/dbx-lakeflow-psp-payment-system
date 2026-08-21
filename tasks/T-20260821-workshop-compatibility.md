---
id: T-20260821-workshop-compatibility
title: "Prove the Free Edition workshop toolchain"
status: blocked
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: any
parent: (none)
depends_on: []
supersedes: (none)
touches_paths: [pyproject.toml]
creates_paths: [toolchain.lock.yaml, uv.lock, tests/compatibility/]
source_note: "docs/plans/workshop-readiness-plan.md sections 2 and 7"
created: "2026-08-21T00:00:00Z"
tags: []
owner: (none)
priority: P2
severity: feature
due_date: (none)
precondition: (none)
blocked_reason: exact-host OAuth and strict bundle validation pass; SQL warehouse and pipeline compute remain unavailable in workspace 647132773346589; retrospective handoff acceptance also failed blast-radius attribution
security_class: (none)
source_action_item: (none)
tracker_ref: (none)
execution_backend: codex
signed_off: true
signed_off_by: luanmorenomaciel
signed_off_at: 2026-08-21T19:58:02Z
accepted: false
accepted_by: (none)
accepted_at: (none)
---

# Prove the Free Edition workshop toolchain

> **Why:** dbldatagen predates Spark 4.2 and DQX 0.16.0 is new, so the exact local and Free Edition tuple must be proven before the workshop is built around it.

## Goal

Pin and verify Databricks CLI, Spark SDP, pipeline environment, DQX, dbldatagen, Python, and Agent Skills versions locally and on the instructor Free Edition workspace.

## Context

The instructor host is supplied through a Databricks profile and never committed. Remote mutation requires separate rehearsal authorization.

## Behavior

- **B-1** — GIVEN a clean local environment and authenticated instructor profile WHEN the compatibility matrix runs THEN every candidate pin has a local and applicable remote pass or an explicit blocker
- **B-2** — GIVEN the same generator seed is executed twice WHEN logical manifests are compared THEN their entity counts, incident IDs, and content digests match

## Success Criteria

```bash
# eval_1: pinned dependency environment is reproducible
eval_1() {
  uv sync --frozen
}

# eval_2: compatibility smoke tests pass
eval_2() {
  uv run pytest -q tests/compatibility
}

# eval_3: exact selected Free Edition profile passes hosted preflight
eval_3() {
  ./scripts/preflight.sh
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "pinned dependency environment is reproducible"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 180
  - id: eval_2
    description: "compatibility smoke tests pass"
    runnable: bash
    check_type: deterministic
    verifies: [B-1, B-2]
    terminal: true
    expected_duration_sec: 600
  - id: eval_3
    description: "exact selected Free Edition profile passes hosted preflight"
    runnable: bash
    check_type: live
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 180
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
eval_1 && eval_2 && eval_3
```

## Rollback Plan

Remove the isolated workshop environment and restore only files changed by this unit.

## Observability Hooks

Record command, version, timestamp, host, state, and sanitized output digest for every compatibility gate.

## Anti-Patterns

- Use floating latest dependency ranges.
- Treat a local import as proof of serverless compatibility.
- Claim Genie Spaces are available before workspace preflight.

## Do-Not-Touch

- `pipelines/`
- `gen/`
- `data/`
- `.claude/settings.local.json`

## Open Questions

(none — this task is fully specified)
