# Data Modeling Quick Reference

> Fast lookup tables. For code examples, see linked files.

## Modeling Approach Decision Matrix (2026)

| Approach | Best For | Complexity | Query Speed | Maintenance | Modern Stack Fit |
|----------|---------|-----------|-------------|-------------|-----------------|
| Star Schema | BI/analytics with known queries | Low | Fast | Medium | High (+ semantic layer) |
| Snowflake Schema | Normalized dimensions, storage-efficient | Medium | Medium | High | Medium |
| Data Vault 2.0 | Enterprise DWH, many sources, audit | High | Slow (needs marts) | Low (automated) | High (AutomateDV/dbt) |
| One Big Table (OBT) | Dashboard-specific, high concurrency | Low | Fastest | High (full rebuilds) | High (cloud DWH) |
| Activity Schema | Event-driven, flexible analytics | Medium | Fast | Low | Medium |

## SCD Type Comparison

| Type | Technique | History | Storage | Complexity |
|------|----------|---------|---------|-----------|
| Type 1 | Overwrite | None | Minimal | Trivial |
| Type 2 | Add row + valid_from/to | Full | High | Medium |
| Type 3 | Add previous_value column | Limited (1 prior) | Low | Low |
| Type 4 | Mini-dimension table | Full (separate) | Medium | Medium |
| Type 6 | Type 1+2+3 hybrid | Full + current flag | Highest | High |

## Normalization Forms

| Form | Rule | When to Use |
|------|------|-------------|
| 1NF | Atomic values, no repeating groups | Always (minimum) |
| 2NF | No partial dependencies on composite key | Transactional systems |
| 3NF | No transitive dependencies | OLTP databases |
| BCNF | Every determinant is a candidate key | Strict data integrity |
| Denormalized | Strategic redundancy | Analytical queries (star schema) |

## Schema Evolution Compatibility (Iceberg v3 / Delta 4.x / Avro)

| Change Type | Backward Compatible | Forward Compatible | Iceberg v3 | Delta 4.x | Avro |
|-------------|--------------------|--------------------|-----------|----------|------|
| Add column (nullable) | Yes | Yes (with default) | `ALTER ADD` | `mergeSchema` | Add with default |
| Add column (NOT NULL) | Depends | Depends | Requires default | Requires default | Breaking |
| Drop column | No | Yes | `ALTER DROP` | `overwriteSchema` | Remove with default |
| Rename column | No | No | `ALTER RENAME` | Column mapping mode | N/A (aliases) |
| Widen type (int->bigint) | Yes | No | `ALTER TYPE` | Type widening (4.0+) | Promotion rules |
| Narrow type (bigint->int) | No | No | Not allowed | Not allowed | Not allowed |
| Reorder columns | No (cosmetic) | No | Supported | N/A | Positional |
| Add Variant column | Yes | Yes | v3 native | 4.0+ native | N/A |

## Key Design Rules

| Rule | Rationale |
|------|-----------|
| Every table has a primary key | Uniqueness guarantee, merge key |
| Facts have numeric measures | Aggregatable, NOT NULL DEFAULT 0 |
| Dimensions have surrogate keys | Insulates from source key changes |
| Date dimension is always separate | Reusable across all facts |
| Grain is documented | Prevents accidental fan-out |

## Star Schema vs OBT Decision Guide (2025+ Debate)

| Factor | Star Schema Wins | OBT Wins |
|--------|-----------------|----------|
| Team has SQL expertise | Moderate-to-expert SQL team | Analysts prefer simple queries |
| Query concurrency | Low-to-moderate concurrency | High concurrency dashboards |
| Dimension reuse | Shared dims across many facts | Single dashboard use case |
| Schema changes | Frequent dimension changes | Stable, rarely changing schema |
| Semantic layer | Yes (abstracts joins) | Not needed |
| Data volume | Any | Storage is cheap (cloud DWH) |
| Best practice | Star schema + semantic layer for flexibility | OBT as a mart layer on top of star schema |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Skip grain definition | Document "one row per ___" on every fact |
| Nullable fact measures | DEFAULT 0 for numeric measures |
| Natural keys as PKs | Surrogate keys (hash or sequence, or identity columns in Delta 4.0+) |
| VARCHAR without limits | Set reasonable max: VARCHAR(256) |
| OBT as your only model | Use OBT as a mart; keep star schema underneath |
| Hand-write all Data Vault SQL | Use AutomateDV / dbt macros for Hub/Link/Sat generation |
| Ignore schema evolution tooling | Use Iceberg/Delta native evolution instead of manual DDL scripts |
