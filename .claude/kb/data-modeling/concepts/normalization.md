# Normalization

> **Purpose**: 1NF-BCNF with examples; when to denormalize; OBT trade-offs for analytics
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Normalization eliminates redundancy in OLTP schemas (1NF → BCNF). Analytics warehouses deliberately denormalize for query performance — fewer joins, wider tables, pre-computed aggregates. The One Big Table (OBT) pattern takes denormalization to its extreme.

## The Concept

```sql
-- 3NF (Normalized): No transitive dependencies
CREATE TABLE customers (
    customer_id    VARCHAR(36) PRIMARY KEY,
    customer_name  VARCHAR(200) NOT NULL,
    region_id      INT REFERENCES regions(region_id)
);

CREATE TABLE regions (
    region_id    INT PRIMARY KEY,
    region_name  VARCHAR(100) NOT NULL,
    country      VARCHAR(100) NOT NULL
);

CREATE TABLE orders (
    order_id     VARCHAR(36) PRIMARY KEY,
    customer_id  VARCHAR(36) REFERENCES customers(customer_id),
    order_date   DATE NOT NULL,
    total_amount DECIMAL(12,2) NOT NULL
);

-- Denormalized for analytics: trade storage for query speed
CREATE TABLE orders_wide AS
SELECT
    o.order_id,
    o.order_date,
    o.total_amount,
    c.customer_name,
    c.customer_id,
    r.region_name,
    r.country
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN regions r ON c.region_id = r.region_id;
```

## Quick Reference

| Normal Form | Rule | Violation Example |
|-------------|------|-------------------|
| 1NF | Atomic values, no repeating groups | `tags = "a,b,c"` in one column |
| 2NF | No partial dependencies (on composite PK) | Non-key depends on part of PK |
| 3NF | No transitive dependencies | `region_name` stored with `customer` |
| BCNF | Every determinant is a candidate key | Rare edge cases beyond 3NF |

| Approach | Joins | Storage | Query Speed | Best For | Cloud DWH Cost |
|----------|-------|---------|-------------|----------|---------------|
| 3NF | Many | Low | Slow (analytics) | OLTP, source systems | High compute |
| Star schema | Few | Medium | Fast | Data warehouse | Balanced |
| OBT | Zero | High | Fastest | BI dashboards, high concurrency | Low compute, high storage |
| Nested/repeated (BigQuery) | Zero (UNNEST) | Medium | Fast | BigQuery-native analytics | Optimized |

## Common Mistakes

### Wrong

```sql
-- Over-normalized analytics table: 6 joins for a simple dashboard query
SELECT f.amount, d.date_name, c.name, p.product_name, s.store_name, r.region_name
FROM fact_sales f
JOIN dim_date d ON f.date_sk = d.date_sk
JOIN dim_customer c ON f.customer_sk = c.customer_sk
JOIN dim_product p ON f.product_sk = p.product_sk
JOIN dim_store s ON f.store_sk = s.store_sk
JOIN dim_region r ON s.region_sk = r.region_sk;
```

### Correct

```sql
-- For dashboard-specific use: pre-joined mart or OBT
-- Trade storage for query simplicity
SELECT amount, order_date, customer_name, product_name, store_name, region_name
FROM mart_sales_summary
WHERE order_date >= CURRENT_DATE - INTERVAL '30 days';
```

## Related

- [dimensional-modeling](dimensional-modeling.md)
- [one-big-table pattern](../patterns/one-big-table.md)
- [star-schema pattern](../patterns/star-schema.md)
