# dbt Knowledge Base

> **Purpose**: dbt development patterns — Fusion Engine, Mesh, Semantic Layer, models, macros, tests
> **MCP Validated**: 2026-03-26 | Updated with dbt Core v1.9/v1.10, Fusion Engine GA status, microbatch strategy

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/model-types.md](concepts/model-types.md) | Staging, intermediate, mart, and snapshot models |
| [concepts/incremental-strategies.md](concepts/incremental-strategies.md) | Append, merge, delete+insert, insert_overwrite |
| [concepts/testing-framework.md](concepts/testing-framework.md) | Generic, singular, and custom test macros |
| [concepts/fusion-engine.md](concepts/fusion-engine.md) | Rust-based compilation, sub-second parsing |
| [concepts/mesh-architecture.md](concepts/mesh-architecture.md) | Multi-project refs, access modifiers, Semantic Layer |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/incremental-model.md](patterns/incremental-model.md) | Production incremental model with merge strategy |
| [patterns/snapshot-scd2.md](patterns/snapshot-scd2.md) | SCD Type 2 snapshots with check_cols/timestamp |
| [patterns/generic-tests.md](patterns/generic-tests.md) | Custom generic test macros and parameterization |
| [patterns/macro-patterns.md](patterns/macro-patterns.md) | Jinja control flow, cross-db macros, grants |
| [patterns/semantic-layer.md](patterns/semantic-layer.md) | MetricFlow metrics, semantic models, exposures |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Model Types** | stg_, int_, fct_, dim_ naming with ref()/source() chains |
| **Incremental** | Avoid full refresh on large tables — merge, append, delete+insert, or **microbatch** (v1.9+) |
| **Fusion Engine** | Rust-based dbt compiler — sub-second parse for 10K+ models, multi-dialect SQL, ADBC drivers |
| **Mesh** | Multi-project architecture with cross-project refs, contracts, and access modifiers |
| **Semantic Layer** | Centralized metrics via MetricFlow, consumed by BI tools and AI agents |
| **Unit Testing** | Validate SQL logic on static inputs before materialization (v1.8+) |
| **Sample Mode** | `--sample` flag for time-based data sampling during dev builds (v1.10+) |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/model-types.md |
| **Intermediate** | patterns/incremental-model.md |
| **Advanced** | concepts/mesh-architecture.md, patterns/semantic-layer.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| dbt-specialist | All files | Model creation, macro dev, test strategy |
| data-quality-analyst | patterns/generic-tests.md | Test generation for dbt models |
| build-agent | patterns/incremental-model.md | Verification via dbt build |
