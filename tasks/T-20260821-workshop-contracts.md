---
id: T-20260821-workshop-contracts
title: "Freeze payment and multi-agent contracts"
status: ready
format_version: 3
profile: full
effort: L
budget_iterations: 15
agent: any
parent: (none)
depends_on: [T-20260821-workshop-compatibility]
supersedes: (none)
touches_paths: []
creates_paths: [AGENTS.md, CLAUDE.md, configs/contracts/, scripts/install_agent_skills.sh, tests/contract/]
source_note: "live landing page and docs/plans/workshop-readiness-plan.md sections 3 and 6"
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

# Freeze payment and multi-agent contracts

> **Why:** Claude Code and Codex need the same plan-before-edit, dev-only, evidence-preserving authority contract.

## Goal

Create one canonical agent contract, two agent-facing projections, an executable four-entity payment contract, and project-scope Databricks Agent Skills bootstrap.

## Context

AGENTS.md is vendor-neutral; CLAUDE.md is the Claude-specific adapter and starts in plan mode.

## Behavior

- **B-1** — GIVEN either Claude Code or Codex reads the repository WHEN it proposes workshop changes THEN it stays within declared paths, preserves quarantine, targets dev, and stops before deploy
- **B-2** — GIVEN project-scope skills are installed WHEN the skill inventory is listed THEN the selected Databricks skills are discoverable for both agents

## Success Criteria

```bash
# eval_1: validate data and agent contracts
eval_1() {
  uv run pytest -q tests/contract
}

# eval_2: verify selected project skills
eval_2() {
  databricks aitools list --scope project
}

```

## Validation Card

```yaml
success_criteria:
  - id: eval_1
    description: "validate data and agent contracts"
    runnable: bash
    check_type: deterministic
    verifies: [B-1]
    terminal: true
    expected_duration_sec: 60
  - id: eval_2
    description: "verify selected project skills"
    runnable: bash
    check_type: deterministic
    verifies: [B-2]
    terminal: true
    expected_duration_sec: 30
retry_policy:
  max_iterations: 15
  circuit_breaker_no_progress: 3
  on_terminal_failure: park_with_context
agent_contract:
  version: 2
  read: [intent, behavior, contract, guardrails]
  produce: [code, tests]
  required_tools: [databricks, claude, codex, pytest, git]
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

Remove generated projections and project-installed Databricks skills without touching existing custom Claude skills.

## Observability Hooks

Skill list and contract digests are captured without credentials.

## Anti-Patterns

- Duplicate independent rules across AGENTS.md and CLAUDE.md.
- Allow agent-driven deployment without human approval.
- Commit tokens or Databricks profile contents.

## Do-Not-Touch

- `.claude/kb/`
- `.claude/agents/`
- `pipelines/`
- `gen/`
- `data/`

## Open Questions

(none — this task is fully specified)
