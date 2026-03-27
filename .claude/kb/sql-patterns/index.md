# SQL Patterns Knowledge Base

> **Purpose**: Cross-dialect SQL — window functions, CTEs, deduplication, DuckDB/Snowflake/BigQuery/Spark
> **MCP Validated**: 2026-03-26 | Updated with DuckDB 1.3+, ASOF JOIN, time_bucket, PIVOT/UNPIVOT enhancements

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/window-functions.md](concepts/window-functions.md) | ROW_NUMBER, RANK, LAG/LEAD, QUALIFY |
| [concepts/cte-patterns.md](concepts/cte-patterns.md) | Recursive, chained, materialized CTEs |
| [concepts/set-operations.md](concepts/set-operations.md) | UNION, LATERAL, UNNEST across dialects |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/deduplication.md](patterns/deduplication.md) | Exact, fuzzy, SCD-aware dedup |
| [patterns/gap-and-island.md](patterns/gap-and-island.md) | Date gaps, session detection |
| [patterns/pivot-unpivot.md](patterns/pivot-unpivot.md) | Cross-dialect PIVOT/UNPIVOT |
| [patterns/cross-dialect.md](patterns/cross-dialect.md) | DuckDB specifics, dialect translation |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Window Functions** | ROW_NUMBER, RANK, LAG/LEAD, QUALIFY for filtering window results |
| **CTEs** | Chained, recursive, and materialized Common Table Expressions |
| **ASOF JOIN** | Temporal lookup — "most recent value as of this time" (DuckDB, Snowflake) |
| **time_bucket()** | Time-series bucketing for aggregation (DuckDB, TimescaleDB) |
| **PIVOT/UNPIVOT** | Row-to-column and column-to-row transformations across dialects |
| **Cross-Dialect** | Portable SQL patterns: DuckDB, Snowflake, BigQuery, Spark SQL |

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| sql-optimizer | All files | Query optimization, dialect translation |
| code-reviewer | patterns/cross-dialect.md | SQL review |
| dbt-specialist | patterns/deduplication.md | Model SQL patterns |
