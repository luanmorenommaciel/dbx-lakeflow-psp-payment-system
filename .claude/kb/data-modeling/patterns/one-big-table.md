# One Big Table (OBT)

> **Purpose**: Denormalized analytics table — when to use, the 2025+ debate, materialization strategy, trade-offs
> **Confidence**: 0.92
> **MCP Validated**: 2026-03-26

## Overview

The One Big Table (OBT) pattern pre-joins all dimensions into a single wide table optimized for BI dashboards. Zero joins at query time means fast queries, but high storage and complex refresh logic. The OBT vs Star Schema debate intensified in 2025 as cloud data warehouses made storage cheap and join overhead became the performance bottleneck for high-concurrency dashboards. The consensus: OBT works as a mart layer on top of star schemas, not as a replacement for dimensional modeling.

## The Pattern

```sql
-- ============================================================
-- OBT: Pre-joined analytics table for e-commerce dashboard
-- Grain: one row = one order line item (fully denormalized)
-- ============================================================

CREATE TABLE obt_order_analytics AS
WITH current_customers AS (
    SELECT * FROM dim_customer WHERE is_current = TRUE
)
SELECT
    -- Fact measures
    f.order_item_sk,
    f.order_id,
    f.quantity,
    f.unit_price,
    f.discount_pct,
    f.net_amount,
    f.tax_amount,
    f.shipping_cost,
    f.net_amount + f.tax_amount + f.shipping_cost AS gross_amount,

    -- Date attributes (denormalized)
    d.full_date        AS order_date,
    d.day_of_week,
    d.month_name,
    d.quarter,
    d.year,
    d.is_weekend,

    -- Customer attributes (denormalized)
    c.customer_id,
    c.customer_name,
    c.segment          AS customer_segment,
    c.region           AS customer_region,
    c.country          AS customer_country,

    -- Product attributes (denormalized)
    p.product_id,
    p.product_name,
    p.category         AS product_category,
    p.subcategory      AS product_subcategory,
    p.brand            AS product_brand,

    -- Computed metrics
    f.net_amount - (f.quantity * p.unit_cost) AS margin

FROM fact_order_items f
JOIN dim_date d ON f.date_sk = d.date_sk
JOIN current_customers c ON f.customer_sk = c.customer_sk
JOIN dim_product p ON f.product_sk = p.product_sk;

-- Incremental refresh strategy (for large tables)
-- Use MERGE or DELETE+INSERT for the changed date partition
DELETE FROM obt_order_analytics WHERE order_date >= CURRENT_DATE - INTERVAL '3 days';
INSERT INTO obt_order_analytics
SELECT /* same query as above */
WHERE d.full_date >= CURRENT_DATE - INTERVAL '3 days';
```

## Quick Reference

| Factor | Star Schema | OBT |
|--------|------------|-----|
| Query complexity | 3-6 JOINs | Zero JOINs |
| Storage | Lower (normalized dims) | Higher (repeated attrs) |
| Refresh complexity | Update dims independently | Must rebuild/merge entire table |
| Flexibility | Ad-hoc joins possible | Fixed set of attributes |
| Best for | Warehouse layer | Specific dashboard / BI tool |
| dbt materialization | table/incremental | table (full refresh) or incremental |

## Common Mistakes

### Wrong

```sql
-- OBT as the only layer — no way to add new dimensions without full rebuild
-- All analytics queries hit one massive table even for simple lookups
SELECT DISTINCT customer_segment FROM obt_order_analytics;  -- scans entire OBT
```

### Correct

```sql
-- OBT as a mart layer, star schema as the foundation
-- Simple lookups still use dimension tables
SELECT DISTINCT segment FROM dim_customer WHERE is_current = TRUE;

-- OBT only for dashboard-specific heavy queries
SELECT customer_segment, SUM(net_amount) FROM obt_order_analytics
WHERE year = 2026 GROUP BY customer_segment;
```

## The 2025+ OBT Debate: Resolution

The data community debated OBT vs Star Schema extensively in 2025. Key takeaways:

| Argument | For OBT | For Star Schema |
|----------|---------|----------------|
| Query simplicity | Zero joins, any analyst can query | Requires SQL skill or semantic layer |
| Performance | Fastest for single-table scans | Join overhead with high concurrency |
| Flexibility | Fixed attributes, adding dims = full rebuild | Ad-hoc joins, easy to extend |
| Consistency | Each OBT is isolated, definitions can drift | Conformed dimensions shared across facts |
| Storage | Higher (repeated attributes) | Lower (normalized dims) |
| Cloud DWH cost | Storage cheap, compute saved | Storage efficient, more compute on joins |

**Best practice (2025+):** Use star schema as the foundation (Silver/Gold layer). Build purpose-specific OBTs as materialized marts for dashboards. Use a semantic layer (dbt Metrics, Cube, AtScale) to abstract join complexity for analysts. This gives the best of both worlds: consistency from star schema, performance from OBT where needed.

```sql
-- Pattern: Star schema foundation + OBT mart
-- Gold layer: star schema (reusable, consistent)
CREATE TABLE gold.fact_orders (...);
CREATE TABLE gold.dim_customer (...);
CREATE TABLE gold.dim_product (...);

-- Mart layer: OBT for specific dashboard (disposable, rebuildable)
CREATE OR REPLACE TABLE mart.obt_executive_dashboard AS
SELECT ... FROM gold.fact_orders
JOIN gold.dim_customer USING (customer_sk)
JOIN gold.dim_product USING (product_sk);
```

## Related

- [star-schema](star-schema.md)
- [dimensional-modeling](../concepts/dimensional-modeling.md)
- [normalization](../concepts/normalization.md)
