# SQL Patterns Quick Reference

> Fast lookup tables. For code examples, see linked files.
> MCP Validated: 2026-03-26 | Updated with DuckDB 1.3+, ASOF JOIN, time_bucket, lambda syntax changes

## Window Function Syntax by Dialect

| Function | Snowflake | BigQuery | DuckDB | Spark SQL |
|----------|-----------|----------|--------|-----------|
| `ROW_NUMBER()` | Yes | Yes | Yes | Yes |
| `QUALIFY` | Yes | Yes | Yes | No |
| `NTILE(n)` | Yes | Yes | Yes | Yes |
| `FIRST_VALUE` | Yes | Yes | Yes | Yes |
| `NTH_VALUE` | Yes | Yes | Yes | Yes |

## CTE Support Matrix

| Feature | Snowflake | BigQuery | DuckDB | Postgres | Spark SQL |
|---------|-----------|----------|--------|----------|-----------|
| Standard CTE | Yes | Yes | Yes | Yes | Yes |
| Recursive CTE | Yes | Yes | Yes | Yes | Limited (3.4+/Databricks) |
| Materialized CTE | No | No | No | Yes | No |
| CTE in DML | Yes | Yes | Yes | Yes | Limited |

## QUALIFY Clause (Filter Window Results)

| Dialect | Support | Alternative |
|---------|---------|-------------|
| Snowflake | Native | -- |
| BigQuery | Native | -- |
| DuckDB | Native | -- |
| Databricks SQL | Native | -- |
| Spark SQL (OSS) | No | Subquery + WHERE |
| Postgres | No | Subquery + WHERE |

## Cross-Dialect Type Mapping

| Concept | Snowflake | BigQuery | DuckDB | Postgres |
|---------|-----------|----------|--------|----------|
| Array | `ARRAY` | `ARRAY<T>` | `T[]` | `T[]` |
| Struct | `OBJECT` | `STRUCT<>` | `STRUCT` | Composite type |
| JSON | `VARIANT` | `JSON` | `JSON` | `jsonb` |
| Flatten | `FLATTEN()` | `UNNEST()` | `unnest()` | `unnest()` |
| Date trunc | `DATE_TRUNC('month', d)` | `DATE_TRUNC(d, MONTH)` | `DATE_TRUNC('month', d)` | `DATE_TRUNC('month', d)` |

## ASOF JOIN (DuckDB, Snowflake)

| Dialect | Syntax | Notes |
|---------|--------|-------|
| DuckDB | `FROM t1 ASOF JOIN t2 ON t1.id = t2.id AND t1.ts >= t2.ts` | Efficient temporal lookup |
| Snowflake | `FROM t1 ASOF JOIN t2 MATCH_CONDITION(t1.ts >= t2.ts)` | Preview feature |
| Others | Subquery + `ROW_NUMBER()` or `LATERAL JOIN` | Manual workaround |

## time_bucket (Time-Series Bucketing)

| Dialect | Syntax | Example |
|---------|--------|---------|
| DuckDB | `time_bucket(INTERVAL '15 min', ts)` | Align sensor data to 15-min intervals |
| TimescaleDB | `time_bucket('15 minutes', ts)` | Same concept, different syntax |
| Snowflake | `TIME_SLICE(ts, 15, 'MINUTE')` | Snowflake equivalent |
| BigQuery | `TIMESTAMP_BUCKET(ts, INTERVAL 15 MINUTE)` | BigQuery equivalent |

## DuckDB 1.3+ Changes

| Change | Details |
|--------|---------|
| Lambda syntax | `x -> x + 1` deprecated; use `(x) -> x + 1` (double arrow planned) |
| ASOF JOIN pipeline | Improved performance for large temporal joins |
| glibc requirement | Official binaries require glibc 2.28+ |
| `COLUMNS()` regex | `SELECT COLUMNS('revenue_.*') FROM tbl` for dynamic column selection |
| List comprehension | `[x * 2 FOR x IN [1,2,3] IF x > 1]` |

## Anti-Pattern Checklist

| Anti-Pattern | Fix |
|-------------|-----|
| `SELECT *` | Explicit column list |
| `DISTINCT` masking duplication | Fix upstream JOIN |
| Correlated subquery | Rewrite as JOIN or window |
| Implicit type cast | Explicit `CAST(col AS type)` |
| `ORDER BY` without `LIMIT` | Add LIMIT or remove ORDER BY |
| `NOT IN (subquery with NULLs)` | Use `NOT EXISTS` instead |
