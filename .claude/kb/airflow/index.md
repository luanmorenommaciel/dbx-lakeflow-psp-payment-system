# Airflow Knowledge Base

> **Purpose**: Orchestration patterns — Airflow 3.x asset-aware scheduling, DAG versioning, remote execution, TaskFlow API
> **MCP Validated**: 2026-03-26 | Updated with Airflow 3.0 GA (April 2025) features

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/dag-design.md](concepts/dag-design.md) | Idempotency, atomicity, retries, SLAs |
| [concepts/operators.md](concepts/operators.md) | Operator catalog, TaskFlow @task, deferrable |
| [concepts/task-dependencies.md](concepts/task-dependencies.md) | Trigger rules, XCom, dynamic task mapping |
| [concepts/orchestrator-comparison.md](concepts/orchestrator-comparison.md) | Airflow vs Dagster vs Prefect 3.x |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/dag-factory.md](patterns/dag-factory.md) | Parameterized DAGs, YAML-driven generation |
| [patterns/sensors-triggers.md](patterns/sensors-triggers.md) | Deferrable operators, external sensors |
| [patterns/dynamic-task-mapping.md](patterns/dynamic-task-mapping.md) | expand(), map(), fan-out/fan-in |
| [patterns/error-handling.md](patterns/error-handling.md) | Retries, SLA callbacks, alerting |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **DAG Design** | Idempotent, atomic tasks with retry and SLA configuration |
| **TaskFlow API** | Modern @task decorators replacing classic operators |
| **Asset-Aware Scheduling** | **Airflow 3.0**: `@asset` decorator and `Asset()` for event-driven DAG triggering |
| **DAG Versioning** | **Airflow 3.0**: Tracks and saves DAG code at execution time |
| **Remote Execution** | **Airflow 3.0**: Task Execution API decouples tasks from scheduler |
| **Dynamic Mapping** | expand()/map() for runtime-determined task parallelism |
| **New UI** | **Airflow 3.0**: React-based UI with asset and task navigation |
| **Orchestrator Choice** | Airflow 3.x vs Dagster vs Prefect 3.x |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/dag-design.md |
| **Intermediate** | patterns/dynamic-task-mapping.md |
| **Advanced** | concepts/orchestrator-comparison.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| pipeline-architect | All files | DAG creation, orchestration design |
| build-agent | patterns/error-handling.md | Pipeline verification |
