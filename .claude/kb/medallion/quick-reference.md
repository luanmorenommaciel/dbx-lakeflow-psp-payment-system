# Medallion Architecture Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated**: 2026-03-26

## Layer Comparison

| Property | Bronze | Silver | Gold | Feature (AI ext.) | Vector (AI ext.) |
|----------|--------|--------|------|-------------------|------------------|
| **Data Quality** | Raw, as-is | Cleansed, validated | Business-ready | ML-ready, point-in-time | Embedding-ready |
| **Schema** | Schema-on-read | Schema-on-write | Star/snowflake | Feature schemas | Vector + metadata |
| **Duplicates** | Allowed | Deduplicated | Aggregated | Versioned features | Deduplicated embeddings |
| **Format** | Delta/Iceberg (append) | Delta/Iceberg (merge) | Delta/Iceberg (overwrite/merge) | Feature store | Vector DB (Qdrant, Pinecone) |
| **Consumers** | Data engineers | Analysts, ML engineers | Business users, dashboards | ML models, inference | RAG, semantic search |
| **Retention** | Long (years) | Medium (months) | Short-medium (refreshed) | Versioned (time-travel) | Refreshed with source |

## Naming Convention

| Layer | Database Pattern | Table Pattern |
|-------|-----------------|---------------|
| Bronze | `bronze_{domain}` | `raw_{source}_{entity}` |
| Silver | `silver_{domain}` | `cleansed_{entity}` |
| Gold | `gold_{domain}` | `dim_{entity}`, `fact_{entity}`, `agg_{metric}` |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Raw event ingestion | Bronze append-only with metadata |
| Deduplication + cleansing | Silver MERGE with dedup window |
| Business KPIs / dashboards | Gold pre-aggregated tables |
| ML feature engineering | Feature layer (or Silver with point-in-time joins) |
| RAG / semantic search | Vector layer sourced from Gold |
| Data quality enforcement | Quality gates between Bronze-Silver (shift-left) |
| Schema changes from source | Schema evolution at Bronze, migrate Silver |
| Historical tracking (SCD2) | Silver layer with valid_from/valid_to |
| Real-time + batch combined | Bronze streaming, Silver/Gold batch |
| AI model training data | Gold certified datasets with data contracts |
| Cross-domain analytics | Gold layer with domain-separated namespaces |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Transform in Bronze | Keep Bronze raw, add only metadata columns |
| Skip deduplication in Silver | Always deduplicate using business keys |
| Build Gold directly from Bronze | Always go Bronze -> Silver -> Gold |
| One monolithic Gold table | Create purpose-specific Gold aggregates |
| Ignore schema evolution | Use Delta Lake/Iceberg schema evolution features |
| Hard-delete in Bronze | Soft-delete or retain all raw records |
| Store secrets in table properties | Use secret scopes or environment variables |
| Treat medallion as "set and forget" | Medallion is a set of contracts -- enforce and monitor |
| Skip quality in Bronze ("it's raw") | Shift-left: validate basic structure at ingestion |
| Silver = Bronze with nicer column names | Silver must cleanse, deduplicate, type-cast, and validate |
| Gold without `_computed_at` metadata | Always track when aggregations were last refreshed |
| No data contracts between layers | Define schemas, SLAs, and ownership at each boundary |
| Ignore AI/ML data needs | Add Feature/Vector layers for ML workloads |

## Essential Commands (Delta Lake / Iceberg)

| Operation | Delta Lake SQL | Iceberg SQL |
|-----------|---------------|-------------|
| Create Bronze table | `CREATE TABLE bronze.raw USING DELTA` | `CREATE TABLE bronze.raw USING ICEBERG` |
| Merge into Silver | `MERGE INTO silver.t USING src ON key ...` | `MERGE INTO silver.t USING src ON key ...` |
| Schema evolution | `ALTER TABLE t SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')` | `ALTER TABLE t ADD COLUMN col TYPE` |
| Optimize | `OPTIMIZE silver.t` (liquid clustering) | `CALL system.rewrite_data_files(...)` |
| Vacuum / Expire | `VACUUM bronze.t RETAIN 168 HOURS` | `CALL system.expire_snapshots(...)` |
| Time travel | `SELECT * FROM silver.t VERSION AS OF 5` | `SELECT * FROM silver.t TIMESTAMP AS OF '...'` |
| Type widening (4.0+) | `ALTER TABLE t ALTER COLUMN col TYPE BIGINT` | `ALTER TABLE t ALTER COLUMN col TYPE BIGINT` |
| Variant data (4.0+/v3) | `SELECT col:path::STRING FROM t` | Variant type support (v3) |

## AI-Era Extensions (2025+)

| Extension Layer | Source | Purpose | Storage |
|----------------|--------|---------|---------|
| **Feature Layer** | Silver/Gold | Versioned, point-in-time correct ML features | Feature store (Feast, Tecton, Databricks) |
| **Vector Layer** | Gold | Embeddings for RAG and semantic search | Vector DB (Qdrant, Pinecone, Weaviate) |
| **Semantic Layer** | Gold | Business metrics as code, reusable definitions | dbt Semantic Layer, Cube, AtScale |

Key principles for AI-era medallion:
- Certified Gold datasets serve as the trusted source for AI/ML training data
- Feature layer enforces point-in-time correctness (no data leakage)
- Vector layer is refreshed when underlying Gold data changes
- Data contracts are mandatory when AI models consume the data
- Quality must shift left: validate structure at Bronze, not just Silver

## Related Documentation

| Topic | Path |
|-------|------|
| Layer Transitions | `patterns/layer-transitions.md` |
| Quality Gates | `patterns/data-quality-gates.md` |
| Full Index | `index.md` |
