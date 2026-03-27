# DuckDB Patterns

> **Purpose**: DuckDB SQL patterns — local file queries, S3 remote, Iceberg integration, ATTACH, CI/CD testing, export
> **MCP Validated**: 2026-03-26

## When to Use

- Querying local Parquet/CSV/JSON files without a warehouse
- Replacing Spark for sub-500GB analytical workloads
- CI/CD data testing (fast, zero-infrastructure)
- Prototyping queries before deploying to production warehouse
- Federated queries across Postgres, SQLite, and files

## Implementation

```sql
-- ============================================================
-- Query local files (glob patterns)
-- ============================================================
-- Single Parquet file
SELECT * FROM 'data/orders.parquet' LIMIT 10;

-- Glob pattern (all Parquet files in directory)
SELECT COUNT(*) AS total_rows FROM 'data/orders/*.parquet';

-- CSV with auto-detection
SELECT * FROM read_csv_auto('data/customers.csv');

-- JSON (newline-delimited)
SELECT * FROM read_json_auto('data/events/*.json');

-- Hive-partitioned directory
SELECT * FROM read_parquet('data/events/**/*.parquet', hive_partitioning=true)
WHERE year = 2026 AND month = 3;

-- ============================================================
-- S3 remote queries
-- ============================================================
INSTALL httpfs;
LOAD httpfs;

SET s3_region = 'us-east-1';
SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';

SELECT * FROM 's3://my-bucket/warehouse/orders/*.parquet'
WHERE order_date >= '2026-03-01';

-- ============================================================
-- Iceberg integration
-- ============================================================
INSTALL iceberg;
LOAD iceberg;

SELECT * FROM iceberg_scan('s3://lakehouse/db/orders');

-- With snapshot (time travel)
SELECT * FROM iceberg_scan('s3://lakehouse/db/orders',
    version => 1234567890);

-- ============================================================
-- ATTACH external databases
-- ============================================================
-- Attach Postgres (read-only federated query)
INSTALL postgres;
LOAD postgres;
ATTACH 'host=localhost dbname=app_db' AS pg (TYPE postgres, READ_ONLY);

SELECT * FROM pg.public.users LIMIT 10;

-- Attach SQLite
ATTACH 'legacy.db' AS sqlite_db (TYPE sqlite);

-- Cross-database join
SELECT u.name, o.total
FROM pg.public.users u
JOIN 'data/orders.parquet' o ON u.id = o.user_id;

-- ============================================================
-- CI/CD testing pattern
-- ============================================================
-- Replace warehouse with DuckDB for fast local tests
-- test_orders.sql
CREATE TABLE test_orders AS
SELECT * FROM read_parquet('tests/fixtures/orders.parquet');

-- Run assertions
SELECT CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL: nulls found' END
FROM test_orders WHERE order_id IS NULL;

SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT order_id) THEN 'PASS' ELSE 'FAIL: duplicates' END
FROM test_orders;

-- ============================================================
-- Export (COPY TO)
-- ============================================================
COPY (SELECT * FROM 'data/orders.parquet' WHERE year = 2026)
TO 'output/orders_2026.parquet' (FORMAT PARQUET, COMPRESSION ZSTD);

COPY (SELECT * FROM 'data/orders.parquet')
TO 'output/orders.csv' (HEADER, DELIMITER ',');
```

## Configuration

| Extension | Purpose | Install |
|-----------|---------|---------|
| `httpfs` | S3/HTTPS file access | `INSTALL httpfs` |
| `iceberg` | Iceberg table scanning | `INSTALL iceberg` |
| `postgres` | Postgres federated queries | `INSTALL postgres` |
| `spatial` | Geometry/GIS functions | `INSTALL spatial` |
| `json` | Advanced JSON processing | Built-in |

## See Also

- [duckdb](../concepts/duckdb.md)
- [local-first-analytics](../patterns/local-first-analytics.md)
- [polars-patterns](../patterns/polars-patterns.md)
