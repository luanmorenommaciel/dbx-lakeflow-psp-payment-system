---
id: T-20260821-workshop-baseline-tracer
title: "Deliver the baseline medallion tracer bullet"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: data-engineer
parent: (none)
depends_on: [T-20260821-workshop-generator]
supersedes: (none)
touches_paths: [pyproject.toml, configs/contracts/psp-payment.contract.yaml]
creates_paths: [pipelines/spark-pipeline.yaml, pipelines/src/psp-agentic/, tests/pipeline/]
source_note: "landing promises for Bronze, Silver, Gold, and Lakeflow"
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

# Deliver the baseline medallion tracer bullet

> **Why:** A complete healthy-data path proves the four-entity model and one-pipeline graph before incident behavior is layered in.

## Goal

Ingest batch-001 through four Bronze streaming tables, valid Silver tables, and gold_merchant_risk using pyspark.pipelines.

## Context

Pipeline definition functions remain declarative and contain no writes, counts, collects, or other actions.

## Behavior

- **B-1** — GIVEN batch-001 is available WHEN the pipeline runs THEN four Bronze tables, valid Silver tables, and gold_merchant_risk materialize in one graph

## Success Criteria

```bash
# eval_1: local pipeline graph is valid
eval_1() {
  ./scripts/sdp.sh dry-run --spec pipelines/spark-pipeline.yaml
}

# eval_2: baseline pipeline assertions pass
eval_2() {
  uv run pytest -q tests/pipeline -k baseline
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "local pipeline graph is valid"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 180
  - id: eval_2
    description: "baseline pipeline assertions pass"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
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
eval_1 && eval_2
```

## Rollback Plan

Remove the workshop pipeline code and local SDP storage.

## Observability Hooks

Assert graph nodes, row counts, metadata columns, and Gold metrics.

## Anti-Patterns

- Create one pipeline per medallion layer.
- Use legacy import dlt in new workshop code.
- Perform side effects in dataset-definition functions.

## Do-Not-Touch

- `pipelines/`
- `gen/`
- `data/`

## Open Questions

(none — this task is fully specified)
