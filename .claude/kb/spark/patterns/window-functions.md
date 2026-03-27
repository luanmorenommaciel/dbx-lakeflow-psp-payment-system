# Window Functions

> **Purpose**: PySpark WindowSpec patterns — ROW_NUMBER, RANK, LAG/LEAD, running totals, sessionization
> **MCP Validated**: 2026-03-26

## When to Use

- Ranking rows within groups (deduplication, top-N)
- Accessing previous/next row values (LAG/LEAD)
- Computing running totals or moving averages
- Sessionization — grouping events by time gaps

## Implementation

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# --- DEDUPLICATION (most common pattern) ---
dedup_window = Window.partitionBy("order_id").orderBy(F.col("updated_at").desc())

deduped = (df
    .withColumn("row_num", F.row_number().over(dedup_window))
    .filter(F.col("row_num") == 1)
    .drop("row_num"))


# --- RANKING ---
rank_window = Window.partitionBy("category").orderBy(F.col("revenue").desc())

ranked = (df
    .withColumn("rank", F.rank().over(rank_window))
    .withColumn("dense_rank", F.dense_rank().over(rank_window))
    .withColumn("pct_rank", F.percent_rank().over(rank_window)))


# --- LAG / LEAD (previous/next values) ---
time_window = Window.partitionBy("customer_id").orderBy("order_date")

with_prev = (df
    .withColumn("prev_order_date", F.lag("order_date", 1).over(time_window))
    .withColumn("next_order_date", F.lead("order_date", 1).over(time_window))
    .withColumn("days_since_last", F.datediff("order_date", "prev_order_date")))


# --- RUNNING TOTALS ---
running_window = (Window
    .partitionBy("customer_id")
    .orderBy("order_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow))

with_running = (df
    .withColumn("cumulative_spend", F.sum("amount").over(running_window))
    .withColumn("order_sequence", F.count("*").over(running_window)))


# --- SESSIONIZATION (gap-based grouping) ---
gap_threshold = 30 * 60  # 30 minutes in seconds

session_window = Window.partitionBy("user_id").orderBy("event_ts")

sessionized = (df
    .withColumn("prev_ts", F.lag("event_ts").over(session_window))
    .withColumn("gap_seconds", F.col("event_ts").cast("long") - F.col("prev_ts").cast("long"))
    .withColumn("new_session", F.when(F.col("gap_seconds") > gap_threshold, 1).otherwise(0))
    .withColumn("session_id", F.sum("new_session").over(session_window)))
```

## Configuration

| Frame | Syntax | Use Case |
|-------|--------|----------|
| `rowsBetween(-1, 1)` | Previous, current, next row | Moving average (3-row) |
| `rowsBetween(unboundedPreceding, currentRow)` | All rows up to current | Running total |
| `rangeBetween(-86400, 0)` | Time-based range (seconds) | 24-hour rolling window |

## Example Usage

```python
# Top-3 products per category by revenue
top3 = (products_df
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("category").orderBy(F.col("revenue").desc())))
    .filter(F.col("rn") <= 3))
```

## See Also

- [dataframe-api](../concepts/dataframe-api.md)
- [performance-tuning](../patterns/performance-tuning.md)
