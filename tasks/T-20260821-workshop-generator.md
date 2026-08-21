---
id: T-20260821-workshop-generator
title: "Build the deterministic dbldatagen incident generator"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: python-developer
parent: (none)
depends_on: [T-20260821-workshop-contracts]
supersedes: (none)
touches_paths: [configs/contracts/]
creates_paths: [gen/synthetic/, tests/generator/, data/fallback/]
source_note: "landing promise for dbldatagen, 100k transactions, and eight incidents"
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

# Build the deterministic dbldatagen incident generator

> **Why:** The current ShadowTraffic seven-entity generator is too broad and is not deterministic in the form the workshop needs.

## Goal

Generate batch-001 and batch-002-incident for four entities using dbldatagen with seed 22082026 and an exact manifest.

## Context

The checked-in fallback pack is produced from the same generator and verified by digest.

## Behavior

- **B-1** — GIVEN seed 22082026 and the payment contract WHEN the generator executes twice THEN both logical manifests match and contain exactly 100000 transactions
- **B-2** — GIVEN batch-002-incident WHEN its manifest is inspected THEN all eight named incident conditions are present with stable sentinel IDs

## Success Criteria

```bash
# eval_1: generator determinism and count tests pass
eval_1() {
  uv run pytest -q tests/generator
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "generator determinism and count tests pass"
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
  required_tools: [uv, pytest, spark-pipelines, databricks]
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

Remove workshop generator outputs and restore the previous lockfile.

## Observability Hooks

Emit counts, seed, batch IDs, incident IDs, and logical content digests.

## Anti-Patterns

- Use random values without a fixed seed.
- Hide incident construction in opaque post-processing.
- Count the late valid chargeback as a rejected row.

## Do-Not-Touch

- `gen/`
- `data/`
- `pipelines/`

## Open Questions

(none — this task is fully specified)
