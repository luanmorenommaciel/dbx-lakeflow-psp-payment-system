# DuckLake

> **Purpose**: DuckDB-based lakehouse with SQL catalog — sub-TB, single-node, local-first, Iceberg interop
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

DuckLake is an open lakehouse format (launched May 2025) where a standard SQL database stores all metadata instead of complex file hierarchies, while data lives in open formats like Parquet on object storage. DuckLake 0.3 (Sep 2025) added Iceberg interoperability (shallow copy between DuckLake and Iceberg), geometry/spatial type support, and improved robustness. The format provides true multi-table ACID transactions, built-in encryption, data inlining for small values, and dramatically simpler operations compared to Iceberg/Delta for sub-TB workloads.

## The Concept

```sql
-- Install and load the DuckLake extension
INSTALL ducklake;
LOAD ducklake;

-- Attach a DuckLake catalog (metadata in DuckDB, data in S3)
ATTACH 'ducklake:s3://my-bucket/ducklake-catalog.db' AS lakehouse;

-- Create a table (data written as Parquet to S3)
CREATE TABLE lakehouse.analytics.events (
    event_id    VARCHAR,
    user_id     VARCHAR,
    event_type  VARCHAR,
    event_ts    TIMESTAMP,
    payload     JSON
);

-- Insert data (written to Parquet files on S3)
INSERT INTO lakehouse.analytics.events
SELECT * FROM read_parquet('s3://raw-data/events/*.parquet');

-- Query with full SQL (DuckDB engine)
SELECT event_type, COUNT(*) AS cnt, DATE_TRUNC('hour', event_ts) AS hour
FROM lakehouse.analytics.events
WHERE event_ts >= '2026-03-01'
GROUP BY event_type, hour
ORDER BY cnt DESC;

-- Time travel (snapshot-based)
SELECT * FROM lakehouse.analytics.events AT (TIMESTAMP '2026-03-25');
```

## Quick Reference

| Feature | DuckLake 0.3 | Iceberg v3 | Delta 4.1 |
|---------|---------|---------|-------|
| Best scale | < 500 GB | TB-PB | TB-PB |
| Catalog | SQL database (DuckDB/Postgres/MySQL) | REST/HMS/Glue | Unity/_delta_log |
| Engine | DuckDB (+ Iceberg bridge) | Multi-engine | Spark-native, some multi |
| Transactions | ACID (multi-table) | ACID (per-table) | ACID (per-table) |
| Time travel | Yes (snapshot-based) | Yes | Yes |
| Partition evolution | N/A (columnar scan) | Yes | No (liquid clustering) |
| Encryption | Built-in (zero-trust) | External (cloud KMS) | External (cloud KMS) |
| Data inlining | Yes (small values stored in metadata) | No | No |
| Iceberg interop | Shallow copy (v0.3) | Native | Via UniForm |
| Geometry/spatial | Yes (v0.3) | Yes (v3) | No |
| Cost | Zero (open source) | Zero + infra | Zero + infra |
| Use case | Dev, CI/CD, local analytics, edge | Production lakehouse | Production lakehouse |

### DuckLake Version History

| Version | Date | Key Features |
|---------|------|-------------|
| 0.1 | May 2025 | Initial release, SQL-based metadata, Parquet data |
| 0.2 | Jul 2025 | Relative schema/table paths, structured file layout |
| 0.3 | Sep 2025 | Iceberg interoperability, geometry support, conflict resolution |

## DuckLake 0.3: Iceberg Interop

```sql
-- Shallow copy from Iceberg to DuckLake (no data rewrite)
INSTALL iceberg;
LOAD iceberg;

-- Copy Iceberg table into DuckLake (metadata-only, references same Parquet files)
COPY FROM DATABASE iceberg_catalog TO ducklake_catalog;

-- Shallow copy from DuckLake to Iceberg
COPY FROM DATABASE ducklake_catalog TO iceberg_catalog;

-- DuckLake with Postgres as metadata backend (multi-user)
ATTACH 'ducklake:postgres:dbname=catalog_db' AS lakehouse;
```

## Common Mistakes

### Wrong

```text
Using DuckLake for a 10 TB production workload with 50 concurrent users.
DuckLake is single-node; use Iceberg/Delta for distributed scale.
```

### Correct

```text
Use DuckLake for:
- Development and CI/CD testing (fast, zero infra)
- Local-first analytics (< 500 GB)
- MotherDuck for cloud sharing of small-medium datasets
- Prototyping before Iceberg/Delta migration
- Edge computing with built-in encryption
- Iceberg bridge: develop in DuckLake, promote to Iceberg for production
```

## Related

- [iceberg-v3](iceberg-v3.md)
- [delta-lake](delta-lake.md)
- [duckdb concept](../../modern-stack/concepts/duckdb.md)
