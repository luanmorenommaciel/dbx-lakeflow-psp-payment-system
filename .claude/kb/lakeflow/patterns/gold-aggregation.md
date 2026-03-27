# Gold Aggregation Patterns

> **MCP Validated**: 2025-01-19
> **Source**: https://docs.databricks.com/aws/en/dlt/materialized-views

## Gold Layer Purpose

Business-ready aggregations and KPIs:

1. **Aggregations** - Sums, counts, averages by dimension
2. **KPIs** - Business metrics and indicators
3. **Denormalized Views** - Joined data for reporting
4. **Time-Series** - Trends and period comparisons

## Payment Transaction KPIs

### Daily Transaction Summary

```python
import dlt
from pyspark.sql import functions as F

@dlt.table(
    name="daily_transaction_summary",
    comment="Daily aggregated transaction metrics",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true"
    }
)
def daily_transaction_summary():
    return (
        spark.read.table("tddf_transactions_silver")
        .groupBy(
            F.col("transaction_date"),
            F.col("merchant_number"),
            F.col("card_type")
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("signed_amount").alias("total_amount"),
            F.avg("signed_amount").alias("avg_amount"),
            F.min("signed_amount").alias("min_amount"),
            F.max("signed_amount").alias("max_amount")
        )
    )
```

### Monthly Merchant Performance

```python
@dlt.table(name="monthly_merchant_performance")
def monthly_merchant_performance():
    return (
        spark.read.table("tddf_transactions_silver")
        .withColumn("month", F.date_trunc("month", F.col("transaction_date")))
        .groupBy("month", "merchant_number")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("signed_amount").alias("total_volume"),
            F.countDistinct("card_number_masked").alias("unique_cards"),
            F.avg("signed_amount").alias("avg_ticket_size")
        )
    )
```

## Merchant Analytics

### Merchant 360 View

```python
@dlt.table(name="merchant_360")
def merchant_360():
    merchants = spark.read.table("mdi_silver")
    transactions = spark.read.table("daily_transaction_summary")

    return (
        merchants
        .join(
            transactions.groupBy("merchant_number").agg(
                F.sum("transaction_count").alias("lifetime_transactions"),
                F.sum("total_amount").alias("lifetime_volume"),
                F.max("transaction_date").alias("last_transaction_date")
            ),
            on="merchant_number",
            how="left"
        )
        .select(
            "merchant_number",
            "merchant_name",
            "dba_name",
            "status",
            "effective_date",
            "lifetime_transactions",
            "lifetime_volume",
            "last_transaction_date"
        )
    )
```

### Fee Analysis

```python
@dlt.table(name="fee_analysis")
def fee_analysis():
    return (
        spark.read.table("tddf_transactions_silver")
        .groupBy("merchant_number", "card_type")
        .agg(
            F.sum("mc_visa_amex_fee").alias("total_network_fees"),
            F.sum("interchange_fee").alias("total_interchange"),
            F.sum("transaction_amount").alias("total_volume"),
            (F.sum("mc_visa_amex_fee") / F.sum("transaction_amount") * 100)
                .alias("effective_rate_pct")
        )
    )
```

## Time Series Patterns

### Rolling 7-Day Average

```python
from pyspark.sql.window import Window

@dlt.table(name="rolling_metrics")
def rolling_metrics():
    window_7d = Window.partitionBy("merchant_number") \
        .orderBy("transaction_date") \
        .rowsBetween(-6, 0)

    return (
        spark.read.table("daily_transaction_summary")
        .withColumn("rolling_7d_volume", F.avg("total_amount").over(window_7d))
        .withColumn("rolling_7d_count", F.avg("transaction_count").over(window_7d))
    )
```

### Month-over-Month Comparison

```python
@dlt.table(name="mom_comparison")
def mom_comparison():
    return (
        spark.read.table("monthly_merchant_performance")
        .withColumn(
            "prev_month_volume",
            F.lag("total_volume").over(
                Window.partitionBy("merchant_number").orderBy("month")
            )
        )
        .withColumn(
            "mom_growth_pct",
            (F.col("total_volume") - F.col("prev_month_volume"))
            / F.col("prev_month_volume") * 100
        )
    )
```

## Data Quality Strategy (Gold)

Gold layer uses FAIL for strict validation:

```python
@dlt.table(name="merchant_kpis")
@dlt.expect_all_or_fail({
    "valid_volume": "total_volume >= 0",
    "valid_count": "transaction_count >= 0",
    "valid_merchant": "merchant_number IS NOT NULL"
})
def merchant_kpis():
    ...
```

## Materialized Views vs Tables

| Use Case | Type | Pattern |
|----------|------|---------|
| Pre-computed aggregates | Materialized View | `@dlt.view()` |
| Incremental updates | Streaming Table | `@dlt.table()` + `read_stream` |
| Historical snapshots | Table | `@dlt.table()` + `read` |

### Materialized View Example

```python
@dlt.view(name="current_merchant_status")
def current_merchant_status():
    return (
        spark.read.table("mdi_silver")
        .filter(F.col("status") == "A")
        .select("merchant_number", "merchant_name", "effective_date")
    )
```

## Related

- [Silver Cleansing](silver-cleansing.md) - Previous layer
- [DABs Deployment](dabs-deployment.md) - Deploy pipelines
- [Materialized Views](../reference/materialized-views.md) - MV deep dive
