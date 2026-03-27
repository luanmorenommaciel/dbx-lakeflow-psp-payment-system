# Lakehouse Quick Reference

> Fast lookup tables. For code examples, see linked files.

## Table Format Comparison (2026)

| Feature | Iceberg v3 | Delta Lake 4.1 | Hudi | DuckLake 0.3 |
|---------|-----------|----------------|------|----------|
| Format war status | **Winner** | Strong #2 (converging via UniForm) | Niche | Emerging |
| Schema evolution | Full + partition | Full + type widening | Full | Full (SQL-native) |
| Partition evolution | Yes (no rewrite) | No (liquid clustering instead) | Limited | N/A (columnar scan) |
| Time travel | Snapshot-based | Version-based | Timeline | Snapshot-based |
| Row-level deletes | Deletion vectors (v3) + position | Deletion vectors | Log-based | Standard SQL |
| Merge-on-read | Yes | Yes (deletion vectors) | Yes | N/A |
| UniForm interop | Native | Reads as Iceberg/Hudi | N/A | Iceberg interop (v0.3) |
| Semi-structured | Variant type (v3) | Variant type (4.0+) | JSON | JSON/nested |
| Geospatial types | Geometry/Geography (v3) | Not native | Not native | Geometry (v0.3) |
| Engine support | Spark, Flink, Trino, DuckDB, Snowflake | Spark, Flink, DuckDB | Spark, Flink | DuckDB (+ Iceberg bridge) |
| v4 / Next | Proposed: content-addressable metadata, streaming commits | Catalog-managed tables, server-side planning | — | Active development |

## Catalog Comparison (2026)

| Feature | Unity Catalog | Apache Gravitino | Nessie | Apache Polaris |
|---------|--------------|-----------------|--------|---------|
| Owner | Databricks | Apache (TLP) | Dremio/Community | Apache (TLP, Feb 2026) |
| Latest version | OSS 0.3.1 | 1.1.0 | 0.95+ | 1.3.0 |
| Multi-format | Delta + Iceberg (UniForm) | Any format + Lance (AI) | Iceberg | Iceberg + generic tables (Delta, Hudi via 1.3) |
| Multi-engine | Spark, DuckDB, Trino | Any engine | Spark, Flink, Trino | Any (REST catalog standard) |
| Git-like branching | No | No | Yes | No |
| Open source | Yes (OSS 0.3.1) | Yes (full ASF) | Yes | Yes (full ASF TLP) |
| ABAC/RBAC | ABAC + tags + data quality monitoring | Unified RBAC (1.0+) + OpenLineage | Basic | RBAC + OPA integration (1.3) |
| AI/ML support | Model registry, feature store | Model Catalog + Lance REST + MCP server | No | Iceberg metrics reporting |
| Production readiness | High | High (1.0 GA) | Medium | High (TLP graduated) |

## When to Use What

| Scenario | Recommended |
|----------|-------------|
| Greenfield lakehouse | Iceberg v3 + Polaris or Gravitino |
| Databricks ecosystem | Delta Lake 4.x + Unity Catalog |
| Multi-engine (Spark + Flink + Trino) | Iceberg + REST catalog (Polaris) |
| Single-node analytics (<500GB) | DuckLake 0.3 or DuckDB + Parquet |
| Need format interop | Delta Lake with UniForm (auto-generates Iceberg metadata) |
| Need git-like data versioning | Iceberg + Nessie |
| AI/ML metadata + vector data | Gravitino 1.1 (Model Catalog + Lance REST) |
| Snowflake + open lakehouse | Iceberg v3 + Polaris |
| Dev/CI/CD testing | DuckLake (zero infra, fast, Iceberg interop) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Skip compaction schedule | OPTIMIZE/rewrite_data_files weekly |
| Unlimited snapshot retention | Set retention period (7-30 days) |
| Ignore small files | Monitor file count, compact when >1000 per partition |
| Choose format by hype | Choose by engine compatibility and team expertise |
| Use HMS as long-term Iceberg catalog | Migrate to REST catalog (Polaris, Gravitino) |
| Ignore UniForm for cross-engine reads | Enable UniForm on Delta tables consumed by non-Spark engines |
| Run DuckLake for >500GB multi-user | Use Iceberg/Delta for distributed scale |
| Skip catalog-managed tables in Delta 4.x | Enable for centralized governance and server-side planning |
