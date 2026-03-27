# Gold Layer

> **Purpose**: Business-level aggregation layer -- star schemas, KPIs, materialized views for consumption
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

The Gold layer contains business-ready data optimized for consumption by dashboards,
reports, ML models, and APIs. It organizes Silver data into dimensional models (star/snowflake
schemas), pre-computes aggregations and KPIs, and provides domain-specific views. Gold tables
are the final presentation layer of the Medallion Architecture.

## The Pattern

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as _sum, count, avg, max as _max,
    current_timestamp, date_trunc
)

spark = SparkSession.builder.getOrCreate()

def build_gold_aggregation(silver_orders: str, silver_customers: str, gold_table: str):
    """Build Gold-layer business aggregation from Silver tables."""
    orders = spark.table(silver_orders)
    customers = spark.table(silver_customers)

    gold_df = (
        orders
        .join(customers, "customer_id", "inner")
        .groupBy(
            date_trunc("month", col("order_date")).alias("order_month"),
            col("customer_segment"),
            col("region")
        )
        .agg(
            _sum("amount").alias("total_revenue"),
            count("order_id").alias("total_orders"),
            avg("amount").alias("avg_order_value"),
            _max("order_date").alias("last_order_date")
        )
        .withColumn("_computed_at", current_timestamp())
    )

    (
        gold_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(gold_table)
    )
    return gold_df
```

## Quick Reference

| Property | Value | Notes |
|----------|-------|-------|
| Write mode | `overwrite` or `merge` | Depends on aggregation type |
| Schema | Star / snowflake | `dim_*` and `fact_*` tables |
| Naming | `dim_{entity}`, `fact_{entity}`, `agg_{metric}` | Business-oriented names |
| Partitioning | By business dimension | Region, date, segment |
| Optimization | Z-ORDER on query columns | Align with common filters |
| Refresh | Scheduled batch | Daily, hourly, or triggered |

## Dimensional Model Example

```sql
-- Dimension: Customers
CREATE OR REPLACE TABLE gold.dim_customers AS
SELECT
    customer_id,
    customer_name,
    email,
    customer_segment,
    region,
    first_order_date,
    lifetime_order_count,
    lifetime_revenue,
    current_timestamp() AS _computed_at
FROM silver.cleansed_customers;

-- Fact: Orders
CREATE OR REPLACE TABLE gold.fact_orders AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.quantity,
    o.unit_price,
    o.quantity * o.unit_price AS total_amount,
    o.discount_pct,
    o.shipping_cost,
    current_timestamp() AS _computed_at
FROM silver.cleansed_orders o;

-- Aggregate: Monthly Revenue KPI
CREATE OR REPLACE TABLE gold.agg_monthly_revenue AS
SELECT
    DATE_TRUNC('month', order_date) AS order_month,
    region,
    customer_segment,
    SUM(total_amount) AS total_revenue,
    COUNT(DISTINCT customer_id) AS unique_customers,
    COUNT(order_id) AS total_orders,
    AVG(total_amount) AS avg_order_value,
    current_timestamp() AS _computed_at
FROM gold.fact_orders f
JOIN gold.dim_customers c USING (customer_id)
GROUP BY 1, 2, 3;
```

## Common Mistakes

### Wrong -- Aggregating from Bronze

```python
# Never skip Silver; you get duplicates and dirty data
bronze_df = spark.table("bronze.raw_orders")
gold_agg = bronze_df.groupBy("region").agg(_sum("amount"))
```

### Correct -- Aggregate from Silver

```python
# Always source from cleansed Silver
silver_df = spark.table("silver.cleansed_orders")
gold_agg = silver_df.groupBy("region").agg(_sum("amount").alias("total_revenue"))
```

## Gold Layer Anti-Patterns

| Anti-Pattern | Problem | Solution |
|-------------|---------|----------|
| One massive Gold table | Slow queries, hard to maintain | Purpose-specific aggregates |
| No `_computed_at` column | Cannot tell when data was refreshed | Always add computation timestamp |
| Raw IDs without dimensions | Users must join manually | Pre-join dimensions into facts |
| No Z-ORDER / liquid clustering | Full scans on filtered queries | Use liquid clustering (Delta 4.x) or Z-ORDER |
| Gold tables business users can't use | Poorly named, no documentation | Business-friendly names + semantic layer |
| No certification/trust markers | Users unsure which tables are reliable | Tag certified Gold tables, add data contracts |
| Ignoring AI/ML consumption | ML models get inconsistent data | Serve certified Gold to feature stores and vector DBs |

## Gold for AI/ML (2025+ Pattern)

```python
# Gold -> Feature Store: serve certified data for ML training
def publish_to_feature_store(gold_table: str, feature_group: str, entity_key: str):
    """Publish Gold layer data as ML features with point-in-time correctness."""
    gold_df = spark.table(gold_table)

    # Feature store expects: entity_key + timestamp + feature columns
    features = (
        gold_df
        .withColumn("event_timestamp", col("_computed_at"))
        .select(entity_key, "event_timestamp", *feature_columns)
    )
    # Write to feature store (Feast, Databricks Feature Store, etc.)
    fs.write_table(name=feature_group, df=features, mode="merge")
```

## Related

- [Silver Layer](../concepts/silver-layer.md)
- [Layer Transitions](../patterns/layer-transitions.md)
- [Domain Modeling](../concepts/domain-modeling.md)
