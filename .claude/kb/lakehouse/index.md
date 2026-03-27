# Lakehouse Knowledge Base

> **Purpose**: Open table formats and catalogs — Iceberg v3 (v4 proposed), Delta Lake 4.1, DuckLake 0.3, Unity Catalog 0.3.1, Gravitino 1.1, Polaris TLP
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/iceberg-v3.md](concepts/iceberg-v3.md) | Format spec, partition evolution, row-level deletes |
| [concepts/delta-lake.md](concepts/delta-lake.md) | Delta 4.1, UniForm, liquid clustering |
| [concepts/catalog-wars.md](concepts/catalog-wars.md) | Unity vs Gravitino vs Nessie vs Polaris |
| [concepts/ducklake.md](concepts/ducklake.md) | DuckDB-based embedded lakehouse |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/iceberg-operations.md](patterns/iceberg-operations.md) | DDL, MERGE, time travel, compaction |
| [patterns/delta-operations.md](patterns/delta-operations.md) | MERGE, OPTIMIZE, vacuum, change data feed |
| [patterns/catalog-setup.md](patterns/catalog-setup.md) | Gravitino, Unity, Nessie configuration |
| [patterns/migration-to-open-formats.md](patterns/migration-to-open-formats.md) | Hive→Iceberg, Parquet→Delta migration |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| lakehouse-architect | All files | Format selection, catalog governance |
| data-platform-engineer | patterns/catalog-setup.md | Infrastructure setup |
