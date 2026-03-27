# Modern Stack Knowledge Base

> **Purpose**: Modern data tools — DuckDB, Polars, SQLMesh, Malloy, Evidence.dev, local-first analytics
> **MCP Validated**: 2026-03-26
> **Latest**: DuckDB 1.2 (Feb 2025) with friendlier SQL, Polars 1.39+ with new streaming engine and sink_batches, SQLMesh 0.231 with Fabric Warehouse support and linter rules

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/duckdb.md](concepts/duckdb.md) | DuckDB 1.2+: columnar engine, extensions, friendlier SQL |
| [concepts/polars.md](concepts/polars.md) | Lazy/eager eval, expression API, Polars Cloud |
| [concepts/sqlmesh.md](concepts/sqlmesh.md) | Virtual environments, auto backfills, dbt compat |
| [concepts/analytics-engineering.md](concepts/analytics-engineering.md) | Malloy, Evidence.dev, Observable |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/duckdb-patterns.md](patterns/duckdb-patterns.md) | S3 queries, Iceberg, CI/CD testing |
| [patterns/polars-patterns.md](patterns/polars-patterns.md) | Lazy pipelines, expression chaining |
| [patterns/sqlmesh-workflow.md](patterns/sqlmesh-workflow.md) | plan/apply, virtual environments |
| [patterns/local-first-analytics.md](patterns/local-first-analytics.md) | DuckDB + Evidence.dev stack |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| data-platform-engineer | All files | Tool selection, local-first patterns |
| sql-optimizer | patterns/duckdb-patterns.md | DuckDB SQL optimization |
