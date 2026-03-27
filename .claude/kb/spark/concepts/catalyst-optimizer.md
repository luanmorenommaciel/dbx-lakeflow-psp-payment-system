# Catalyst Optimizer

> **Purpose**: Spark query optimization — logical plan, physical plan, predicate pushdown, whole-stage codegen, AQE
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Catalyst is Spark's query optimizer that transforms logical plans into optimized physical execution plans. It applies rule-based and cost-based optimizations: predicate pushdown, column pruning, join reordering, and whole-stage code generation. AQE (Adaptive Query Execution) extends this at runtime by adjusting plans based on actual data statistics.

## The Concept

```python
# View the optimized plan to understand what Catalyst does
df = (
    spark.read.parquet("s3://bucket/orders/")
    .filter(F.col("order_date") >= "2026-01-01")
    .select("order_id", "customer_id", "amount")
    .join(customers_df, "customer_id")
)

# Logical plan — what you wrote
df.explain(mode="simple")

# Physical plan — what Spark actually executes
df.explain(mode="extended")

# Formatted plan with statistics (Spark 3.x)
df.explain(mode="formatted")
```

## Quick Reference

| Optimization | What It Does | When It Helps |
|-------------|-------------|---------------|
| Predicate pushdown | Moves filters to data source | Parquet/Delta/Iceberg skip row groups |
| Column pruning | Reads only needed columns | Columnar formats skip unused columns |
| Join reordering | Smaller table on build side | Reduces shuffle and memory |
| Broadcast join | Sends small table to all executors | Avoids shuffle entirely (<10MB default) |
| Whole-stage codegen | Fuses operators into single JVM method | Eliminates virtual function calls |
| AQE coalesce | Merges small post-shuffle partitions | Reduces task overhead |
| AQE skew join | Splits skewed partitions | Prevents straggler tasks |

## Common Mistakes

### Wrong

```python
# Defeats predicate pushdown — filter AFTER complex transform
df = spark.read.parquet("orders/")
df = df.withColumn("year", F.year("order_date"))
df = df.filter(F.col("year") == 2026)  # pushdown blocked by derived column
```

### Correct

```python
# Filter on raw column — Catalyst pushes to Parquet reader
df = spark.read.parquet("orders/")
df = df.filter(F.col("order_date") >= "2026-01-01")
df = df.withColumn("year", F.year("order_date"))
```

## Spark 4.0 SQL Enhancements

### PIPE Syntax (Readable Query Chaining)

```sql
-- Traditional (nested CTEs)
WITH filtered AS (SELECT * FROM orders WHERE amount > 100),
     grouped AS (SELECT region, SUM(amount) AS total FROM filtered GROUP BY region)
SELECT * FROM grouped ORDER BY total DESC;

-- Spark 4.0 PIPE syntax — reads top-to-bottom
FROM orders
|> WHERE amount > 100
|> AGGREGATE SUM(amount) AS total GROUP BY region
|> ORDER BY total DESC;
```

### SQL User-Defined Functions

```sql
-- Reusable SQL UDFs (no Python/Scala needed)
CREATE FUNCTION discount_price(price DECIMAL, pct DECIMAL)
RETURNS DECIMAL
RETURN price * (1 - pct / 100);

SELECT order_id, discount_price(amount, 10) AS discounted FROM orders;
```

### SQL Scripting (Control Flow)

```sql
-- Session variables + control flow in pure SQL
DECLARE VARIABLE threshold = 1000;

BEGIN
  IF (SELECT COUNT(*) FROM staging.orders) > threshold THEN
    INSERT INTO production.orders SELECT * FROM staging.orders;
  ELSE
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Below threshold';
  END IF;
END;
```

## Related

- [partitioning](../concepts/partitioning.md)
- [performance-tuning](../patterns/performance-tuning.md)
