> **MCP Validated:** 2026-02-17

# Warehouse Basics

> **Purpose**: Fabric Data Warehouse fundamentals -- T-SQL engine, security, and analytics
> **Confidence**: 0.95

## Overview

The Fabric Data Warehouse is a full-fidelity T-SQL engine built on OneLake. Unlike the Lakehouse SQL endpoint (read-only), the Warehouse supports full DML operations (INSERT, UPDATE, DELETE, MERGE), stored procedures, views, and security features like row-level security (RLS) and dynamic data masking. Data is stored in Delta Parquet format with automatic V-Order optimization.

## The Pattern

```sql
-- Create a warehouse table with distribution
CREATE TABLE dbo.fact_sales (
    sale_id         BIGINT NOT NULL,
    product_id      INT NOT NULL,
    customer_id     INT NOT NULL,
    sale_date       DATE NOT NULL,
    quantity        INT NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    total_amount    AS (quantity * unit_price),
    region          VARCHAR(50) NOT NULL,
    loaded_at       DATETIME2 DEFAULT GETUTCDATE()
);

-- Create dimension table
CREATE TABLE dbo.dim_product (
    product_id      INT NOT NULL,
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    brand           VARCHAR(100)
);

-- Create a view for reporting
CREATE VIEW dbo.vw_sales_summary AS
SELECT
    p.category,
    p.brand,
    YEAR(s.sale_date) AS sale_year,
    MONTH(s.sale_date) AS sale_month,
    SUM(s.total_amount) AS total_revenue,
    COUNT(*) AS transaction_count
FROM dbo.fact_sales s
JOIN dbo.dim_product p ON s.product_id = p.product_id
GROUP BY p.category, p.brand, YEAR(s.sale_date), MONTH(s.sale_date);
```

## Quick Reference

| Feature | Supported | Notes |
|---------|-----------|-------|
| INSERT/UPDATE/DELETE | Yes | Full DML |
| MERGE (UPSERT) | Yes | SCD Type 1/2 patterns |
| Stored procedures | Yes | T-SQL only (no CLR) |
| Views | Yes | Standard + materialized (preview) |
| Cross-database queries | Yes | Three-part naming |
| Transactions | Yes | ACID compliance |
| Row-level security | Yes | CREATE SECURITY POLICY |
| Dynamic data masking | Yes | MASKED WITH FUNCTION |
| Column-level security | Yes | GRANT/DENY on columns |
| Indexes | No | Engine auto-optimizes |
| Triggers | No | Use pipelines instead |

## Common Mistakes

### Wrong

```sql
-- Trying to create indexes (not supported in Fabric Warehouse)
CREATE INDEX ix_sales_date ON dbo.fact_sales (sale_date);
```

### Correct

```sql
-- Fabric Warehouse auto-optimizes with V-Order; no manual indexes needed
-- Use partitioning via pipeline patterns for large tables instead
```

## Related

- [T-SQL Patterns](../patterns/t-sql-patterns.md)
- [Workload Selection](../../03-architecture-patterns/concepts/workload-selection.md)
- [RLS Security](../../06-governance-security/concepts/rls-security.md)
