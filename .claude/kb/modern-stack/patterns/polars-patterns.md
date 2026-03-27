# Polars Patterns

> **Purpose**: Polars lazy pipelines, expression API, window functions, Arrow/Pandas interop, streaming large files
> **MCP Validated**: 2026-03-26

## When to Use

- DataFrames processing faster than Pandas (multi-threaded, zero-copy)
- Lazy evaluation for query optimization before execution
- Processing larger-than-RAM datasets with streaming mode
- Arrow-native workflows with zero-copy interop

## Implementation

```python
import polars as pl

# ============================================================
# Lazy pipeline (optimized before execution)
# ============================================================
result = (
    pl.scan_parquet("data/orders/*.parquet")
    .filter(pl.col("order_date") >= pl.lit("2026-01-01"))
    .with_columns(
        pl.col("net_amount").cast(pl.Float64).alias("amount_f64"),
        (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount"),
        pl.col("order_date").dt.month().alias("order_month"),
    )
    .group_by("customer_id", "order_month")
    .agg(
        pl.col("net_amount").sum().alias("total_revenue"),
        pl.col("order_id").n_unique().alias("order_count"),
        pl.col("net_amount").mean().alias("avg_order_value"),
    )
    .sort("total_revenue", descending=True)
    .collect()  # execution happens here
)

# ============================================================
# Expression API (chainable, composable)
# ============================================================
df = pl.read_parquet("data/customers.parquet")

cleaned = df.with_columns(
    pl.col("email").str.to_lowercase().str.strip_chars(),
    pl.col("name").str.strip_chars().alias("clean_name"),
    pl.col("revenue")
        .fill_null(0)
        .clip(0, 1_000_000)
        .alias("revenue_clipped"),
    pl.when(pl.col("segment").is_null())
        .then(pl.lit("Unknown"))
        .otherwise(pl.col("segment"))
        .alias("segment_filled"),
)

# ============================================================
# Window functions (.over)
# ============================================================
with_rankings = df.with_columns(
    # Rank within each segment
    pl.col("revenue")
        .rank(descending=True)
        .over("segment")
        .alias("rank_in_segment"),

    # Running total within segment
    pl.col("revenue")
        .cum_sum()
        .over("segment")
        .alias("cumulative_revenue"),

    # Percentage of segment total
    (pl.col("revenue") / pl.col("revenue").sum().over("segment") * 100)
        .round(2)
        .alias("pct_of_segment"),
)

# ============================================================
# Join patterns
# ============================================================
orders = pl.scan_parquet("data/orders.parquet")
customers = pl.scan_parquet("data/customers.parquet")

enriched = (
    orders
    .join(customers, on="customer_id", how="left")
    .filter(pl.col("segment") == "Enterprise")
    .collect()
)

# ============================================================
# Arrow / Pandas interop (zero-copy where possible)
# ============================================================
# Polars → Arrow (zero-copy)
arrow_table = df.to_arrow()

# Arrow → Polars (zero-copy)
df_from_arrow = pl.from_arrow(arrow_table)

# Polars → Pandas
pandas_df = df.to_pandas()

# Pandas → Polars
df_from_pandas = pl.from_pandas(pandas_df)

# ============================================================
# Streaming large files (larger than RAM)
# ============================================================
streamed = (
    pl.scan_csv("data/huge_file.csv")
    .filter(pl.col("status") == "active")
    .group_by("region")
    .agg(pl.col("amount").sum())
    .collect(streaming=True)  # processes in batches
)

# ============================================================
# Multiple source reading
# ============================================================
# Read all Parquet files with hive partitioning
df = pl.scan_parquet(
    "data/events/**/*.parquet",
    hive_partitioning=True,
).collect()
```

## Configuration

| Feature | Polars | Pandas |
|---------|--------|--------|
| Execution | Multi-threaded | Single-threaded |
| Memory | Apache Arrow (columnar) | NumPy (row-ish) |
| Lazy eval | Native (scan → collect) | No |
| Null handling | First-class (no NaN confusion) | NaN/None mixed |
| String type | Arrow UTF-8 | Python objects |
| Index | No index (by design) | Row index |

## See Also

- [polars](../concepts/polars.md)
- [duckdb-patterns](../patterns/duckdb-patterns.md)
- [local-first-analytics](../patterns/local-first-analytics.md)
