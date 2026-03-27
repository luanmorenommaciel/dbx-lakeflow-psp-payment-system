> **MCP Validated:** 2026-02-17

# Medallion Architecture in Fabric

> **Purpose**: Implementing bronze/silver/gold data layers using Fabric Lakehouses and Warehouses

## When to Use

- Building an enterprise data platform with incremental quality improvement
- Separating raw ingestion from curated business analytics
- Enabling data lineage and reprocessing from raw sources
- Multi-team environments where data engineers and analysts work on different layers

## Implementation

```python
# Bronze Layer: Raw ingestion (keep data as-is)
# Notebook: bronze_ingest.py
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# Read raw files from external source
df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/landing/sales/*.csv")

# Add audit columns only -- no transformations
df_bronze = df_raw \
    .withColumn("_ingested_at", current_timestamp()) \
    .withColumn("_source_file", input_file_name()) \
    .withColumn("_source_system", lit("erp"))

# Write to bronze lakehouse
df_bronze.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("bronze_sales")

# -----------------------------------------------------------
# Silver Layer: Cleansed and conformed
# Notebook: silver_transform.py
from pyspark.sql.functions import col, trim, upper, to_date, when

df_bronze = spark.sql("SELECT * FROM bronze_lakehouse.bronze_sales")

df_silver = df_bronze \
    .filter(col("order_id").isNotNull()) \
    .withColumn("customer_name", trim(upper(col("customer_name")))) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd")) \
    .withColumn("amount", col("amount").cast("decimal(18,2)")) \
    .withColumn("amount", when(col("amount") < 0, 0).otherwise(col("amount"))) \
    .dropDuplicates(["order_id"]) \
    .withColumn("_processed_at", current_timestamp())

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("silver_sales")

spark.sql("OPTIMIZE silver_sales VORDER")

# -----------------------------------------------------------
# Gold Layer: Business-ready aggregations
# Notebook: gold_aggregate.py
df_gold = spark.sql("""
    SELECT
        date_format(order_date, 'yyyy-MM') AS sale_month,
        region,
        product_category,
        COUNT(DISTINCT customer_id) AS unique_customers,
        COUNT(*) AS total_orders,
        SUM(amount) AS total_revenue,
        AVG(amount) AS avg_order_value
    FROM silver_lakehouse.silver_sales
    GROUP BY
        date_format(order_date, 'yyyy-MM'),
        region,
        product_category
""")

df_gold.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_sales_summary")
```

## Configuration

| Setting | Recommendation | Description |
|---------|----------------|-------------|
| Bronze workspace | `ws-bronze-{env}` | Separate workspace per layer |
| Silver workspace | `ws-silver-{env}` | Enables granular access control |
| Gold workspace | `ws-gold-{env}` | BI users access only this layer |
| V-Order | Silver + Gold | Optimize after writes for read perf |
| Schema evolution | Bronze only | Use `mergeSchema` on append |
| Deduplication | Silver layer | `dropDuplicates()` on business keys |

## Architecture Patterns

```text
Pattern 1: All-Lakehouse (Spark-first)
  [Sources] --> [Bronze LH] --> [Silver LH] --> [Gold LH] --> [Power BI]

Pattern 2: Lakehouse + Warehouse (SQL-first gold)
  [Sources] --> [Bronze LH] --> [Silver LH] --> [Gold WH] --> [Power BI]
                                                    |
                                                   RLS + Masking

Pattern 3: With Real-Time Layer
  [Sources] --> [Bronze LH] --> [Silver LH] --> [Gold LH]
  [Streams] --> [Eventhouse] ----------------------^
```

## Example Usage

```sql
-- Gold layer in Warehouse (Pattern 2): Materialized view
CREATE VIEW dbo.vw_monthly_revenue AS
SELECT
    FORMAT(s.order_date, 'yyyy-MM') AS sale_month,
    p.category,
    SUM(s.amount) AS revenue,
    COUNT(*) AS orders
FROM silver_lakehouse.dbo.silver_sales s
CROSS DATABASE JOIN gold_warehouse.dbo.dim_product p
    ON s.product_id = p.product_id
GROUP BY FORMAT(s.order_date, 'yyyy-MM'), p.category;
```

## See Also

- [Workload Selection](../concepts/workload-selection.md)
- [Lakehouse](../../02-data-engineering/concepts/lakehouse.md)
- [Copy Activity](../../02-data-engineering/patterns/copy-activity.md)
