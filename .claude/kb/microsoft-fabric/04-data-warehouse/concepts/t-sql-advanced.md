> **MCP Validated:** 2026-02-17

# Advanced T-SQL in Fabric Warehouse

> **Purpose**: CTAS, COPY INTO, cross-database queries, system functions, and DMVs
> **Confidence**: 0.95

## Overview

Fabric Warehouse supports a rich subset of T-SQL beyond basic DML. This includes `CREATE TABLE AS SELECT` (CTAS) for fast materialization, `COPY INTO` for bulk file ingestion, cross-database queries via three-part naming, and dynamic management views (DMVs) for monitoring.

## CTAS (CREATE TABLE AS SELECT)

CTAS creates a new table from a query result. It is the fastest way to materialize transformations, using parallel writes with automatic V-Order.

```sql
-- Materialize a silver-to-gold transformation
CREATE TABLE dbo.gold_monthly_revenue
AS
SELECT
    FORMAT(order_date, 'yyyy-MM') AS sale_month,
    region, category,
    SUM(amount) AS total_revenue,
    COUNT(*) AS order_count
FROM dbo.silver_orders s
JOIN dbo.dim_product p ON s.product_id = p.product_id
GROUP BY FORMAT(order_date, 'yyyy-MM'), region, category;

-- Replace existing table pattern (drop + recreate)
DROP TABLE IF EXISTS dbo.gold_monthly_revenue;
CREATE TABLE dbo.gold_monthly_revenue AS SELECT /* same query */;
```

## COPY INTO

Bulk-loads data from OneLake or external storage into Warehouse tables.

```sql
-- Load Parquet files from OneLake
COPY INTO dbo.stg_orders
FROM 'https://onelake.dfs.fabric.microsoft.com/{ws}/{lh}/Files/landing/*.parquet'
WITH (FILE_TYPE = 'PARQUET', CREDENTIAL = (IDENTITY = 'Managed Identity'));

-- Load CSV with options
COPY INTO dbo.stg_customers
FROM 'https://onelake.dfs.fabric.microsoft.com/{ws}/{lh}/Files/customers.csv'
WITH (
    FILE_TYPE = 'CSV', FIRSTROW = 2,
    FIELDTERMINATOR = ',', ROWTERMINATOR = '\n',
    CREDENTIAL = (IDENTITY = 'Managed Identity')
);
```

## Cross-Database Queries

Query across Warehouses and Lakehouses using three-part naming: `database.schema.table`.

```sql
-- Join Warehouse with Lakehouse
SELECT w.customer_name, w.segment, l.order_count, l.lifetime_value
FROM gold_warehouse.dbo.dim_customer w
JOIN silver_lakehouse.dbo.customer_metrics l
    ON w.customer_id = l.customer_id
WHERE l.lifetime_value > 5000;
```

## System Functions and DMVs

```sql
-- System functions
SELECT GETUTCDATE() AS current_utc;
SELECT DB_NAME() AS current_database;

-- Table metadata
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo';

-- Column metadata
SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'fact_sales';

-- Active queries (DMV)
SELECT session_id, login_name, status, command, total_elapsed_time
FROM sys.dm_exec_requests WHERE status = 'running';

-- Query text for a session (DMV)
SELECT r.session_id, r.status, t.text AS query_text
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t;
```

## Feature Support Summary

| Feature | Supported | Notes |
|---------|-----------|-------|
| CTAS | Yes | Fastest materialization method |
| COPY INTO | Yes | Bulk load from OneLake / external |
| Three-part naming | Yes | Cross-database queries |
| INFORMATION_SCHEMA | Yes | Table and column metadata |
| sys.dm_exec_* DMVs | Yes | Session and query monitoring |
| CTEs / Window funcs | Yes | ROW_NUMBER, RANK, LAG, LEAD |
| JSON functions | Yes | JSON_VALUE, OPENJSON |
| OPENROWSET | No | Use COPY INTO or shortcuts |
| Linked servers | No | Use cross-database queries |
| CLR procedures | No | T-SQL only |

## Common Mistakes

### Wrong

```sql
-- Using INSERT...SELECT for large materializations
INSERT INTO dbo.gold_summary SELECT ... FROM dbo.silver_data;
```

### Correct

```sql
-- Use CTAS for full table rebuilds (parallel + V-Order)
DROP TABLE IF EXISTS dbo.gold_summary;
CREATE TABLE dbo.gold_summary AS SELECT ... FROM dbo.silver_data;
```

## Related

- [Warehouse Basics](warehouse-basics.md)
- [Direct Lake](direct-lake.md)
- [T-SQL Patterns](../patterns/t-sql-patterns.md)
- [Star Schema](../patterns/star-schema.md)
