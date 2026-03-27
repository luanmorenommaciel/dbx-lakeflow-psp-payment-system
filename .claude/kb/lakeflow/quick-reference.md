# Lakeflow Quick Reference

> Fast lookup tables for Lakeflow Declarative Pipelines (GA, 2025+). For code examples, see linked files.

## Python Decorators

| Decorator | Purpose | Action |
|-----------|---------|--------|
| `@dlt.table()` | Create streaming/materialized table | - |
| `@dlt.view()` | Create temporary view | - |
| `@dlt.expect()` | Data quality check | WARN |
| `@dlt.expect_or_drop()` | Drop invalid rows | DROP |
| `@dlt.expect_or_fail()` | Fail pipeline | FAIL |
| `@dlt.expect_all()` | Multiple checks | WARN |
| `@dlt.expect_all_or_drop()` | Multiple checks | DROP |

## SQL Keywords

| Keyword | Purpose |
|---------|---------|
| `CREATE OR REFRESH STREAMING TABLE` | Streaming table |
| `CREATE OR REFRESH MATERIALIZED VIEW` | Batch aggregation |
| `STREAM read_files()` | Auto Loader |
| `APPLY CHANGES INTO` | CDC processing |
| `CONSTRAINT ... EXPECT` | Data quality |

## Functions

| Function | Purpose |
|----------|---------|
| `dlt.read()` | Read table (batch) |
| `dlt.read_stream()` | Read table (streaming) |
| `dlt.apply_changes()` | CDC processing |
| `dlt.create_auto_cdc_flow()` | AUTO CDC flow (2025.30+) |

## AUTO CDC Enhancements (2025.30+)

| Feature | Detail |
|---------|--------|
| Multiple flows per target | `name` parameter on `create_auto_cdc_flow` |
| One-time backfill | `once=True` for initial hydration |
| SQL support | `CREATE AUTO CDC FLOW` in SQL |
| SCD Type 1 materialization | Upsert-only CDC without history (Feb 2026) |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Real-time data | Streaming Table |
| Batch aggregations | Materialized View |
| Track current state | SCD Type 1 |
| Track history | SCD Type 2 |
| Cloud storage | Auto Loader |
| Dev testing | Development mode |
| Production | Serverless + triggered |

## Quality Check Strategy

| Layer | Action | Use Case |
|-------|--------|----------|
| Bronze | WARN | Track issues |
| Silver | DROP | Remove bad data |
| Gold | FAIL | Critical validation |

## New Features (2025-2026)

| Feature | Release | Detail |
|---------|---------|--------|
| Multi-catalog/schema | 2025.04 | Publish to multiple catalogs, LIVE schema optional |
| Move Tables | 2025.29 (GA) | Move MVs/STs between pipelines via SQL |
| ALTER on MVs/STs | 2025.29 | Table comments, column comments, RLS/CLM |
| AUTO CDC multiple flows | 2025.30 | Multiple CDC flows writing to same target |
| AUTO CDC backfill | 2025.30 | One-time `once=True` for initial data load |
| Type widening | Feb 2026 | INT to LONG, FLOAT to DOUBLE without reset |
| SCD Type 1 AUTO CDC | Feb 2026 | Simpler CDC without full change history |
| Serverless TCO -70% | Jul 2025 | Optimized serverless pipelines |
| Spark Declarative Pipelines | DAIS 2025 | Open-sourced to Apache Spark project |

## Troubleshooting

| Error | Solution |
|-------|----------|
| Permission denied | `GRANT USE CATALOG/SCHEMA` |
| Table already defined | Use different name |
| Slow startup | Use performance mode |
| High costs | Switch to triggered |
| Schema evolution | Use materialized view or type widening (2026) |
| Move table between pipelines | `ALTER TABLE ... SET PIPELINE ...` (2025.29+) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Actions in pipeline code | Keep operations lazy |
| Hardcode environments | Use parameters |
| Skip quality checks | Apply at each layer |
| Use reserved properties | Use proper clauses |
| Use LIVE schema (legacy) | Specify target catalog/schema directly (2025.04+) |
| Full pipeline reset for type changes | Use type widening (Feb 2026+) |

**Reserved properties**: `owner`, `location`, `provider`, `external`

## Related Documentation

| Topic | Path |
|-------|------|
| Getting Started | `concepts/getting-started.md` |
| Python API | `patterns/python-api.md` |
| SQL Syntax | `patterns/sql-syntax.md` |
| CDC | `patterns/cdc.md` |
| Data Quality | `patterns/data-quality.md` |
| Configuration | `reference/pipeline-configuration.md` |
| Full Index | `index.md` |
