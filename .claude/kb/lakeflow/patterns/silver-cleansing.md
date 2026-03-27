# Silver Cleansing Patterns

> **MCP Validated**: 2025-01-19
> **Source**: https://docs.databricks.com/aws/en/dlt/expectations

## Silver Layer Purpose

Transform Bronze raw data into clean, typed, validated data:

1. **Type Casting** - Enforce correct data types
2. **Null Handling** - Apply defaults or filter
3. **PII Protection** - Mask or exclude sensitive fields
4. **Deduplication** - Remove duplicate records
5. **Standardization** - Normalize formats

## MDI Silver Pattern

```python
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType

@dlt.table(
    name="mdi_silver",
    comment="Cleaned MDI merchant data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect_or_drop("valid_merchant", "merchant_number IS NOT NULL")
@dlt.expect_or_drop("valid_status", "status IN ('A', 'C', 'D', 'I')")
@dlt.expect("has_bank_number", "bank_number IS NOT NULL")
def mdi_silver():
    return (
        dlt.read_stream("mdi_bronze")
        .select(
            F.col("merchant_number").cast("string"),
            F.col("merchant_name").cast("string"),
            F.col("bank_number").cast("string"),
            F.col("status").cast("string"),
            F.col("effective_date").cast(DateType()),
            F.col("dba_name").cast("string"),
            F.col("_ingested_at"),
            F.col("_source_file")
        )
        .withColumn("_cleaned_at", F.current_timestamp())
    )
```

## TDDF Silver Pattern (Amount Handling)

```python
@dlt.table(name="tddf_transactions_silver")
@dlt.expect_or_drop("valid_amount", "transaction_amount IS NOT NULL")
@dlt.expect_or_drop("valid_merchant", "merchant_number IS NOT NULL")
@dlt.expect("amount_positive", "transaction_amount >= 0 OR dc_indicator = 'C'")
def tddf_transactions_silver():
    return (
        dlt.read_stream("tddf_transactions_bronze")
        .select(
            F.col("merchant_number"),
            F.col("transaction_date").cast(DateType()),
            F.col("transaction_amount").cast(DecimalType(18, 2)),
            F.col("dc_indicator"),
            F.col("card_type"),
            F.col("authorization_code"),
            F.when(F.col("dc_indicator") == "C", -F.col("transaction_amount"))
             .otherwise(F.col("transaction_amount"))
             .alias("signed_amount"),
            F.col("_ingested_at")
        )
        .withColumn("_cleaned_at", F.current_timestamp())
    )
```

## PII Handling Patterns

### Exclude PII Columns

```python
@dlt.table(name="mdi_silver_no_pii")
def mdi_silver_no_pii():
    return (
        dlt.read_stream("mdi_bronze")
        .drop("owner_ssn", "tax_id", "bank_account_number")
        .withColumn("_cleaned_at", F.current_timestamp())
    )
```

### Mask PII Columns

```python
@dlt.table(name="mdi_silver_masked")
def mdi_silver_masked():
    return (
        dlt.read_stream("mdi_bronze")
        .withColumn(
            "tax_id_masked",
            F.concat(F.lit("***-**-"), F.substring(F.col("tax_id"), -4, 4))
        )
        .withColumn(
            "card_number_masked",
            F.concat(F.lit("****-****-****-"), F.substring(F.col("card_number"), -4, 4))
        )
        .drop("tax_id", "card_number")
    )
```

## Deduplication Patterns

### Simple Deduplication

```python
@dlt.table(name="mdi_silver_deduped")
def mdi_silver_deduped():
    return (
        dlt.read_stream("mdi_bronze")
        .dropDuplicates(["merchant_number", "effective_date"])
    )
```

### Keep Latest Record

```python
from pyspark.sql.window import Window

@dlt.table(name="mdi_silver_latest")
def mdi_silver_latest():
    window = Window.partitionBy("merchant_number").orderBy(F.desc("_ingested_at"))
    return (
        dlt.read("mdi_bronze")
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
```

## Data Quality Strategy (Silver)

Silver layer uses DROP to remove invalid records:

```python
@dlt.expect_or_drop("not_null_key", "merchant_number IS NOT NULL")
@dlt.expect_or_drop("valid_date", "transaction_date >= '2020-01-01'")
@dlt.expect_or_drop("valid_amount", "amount > 0")
```

## Type Casting Reference

| Source Type | Target Type | Pattern |
|-------------|-------------|---------|
| String amount | Decimal | `.cast(DecimalType(18, 2))` |
| String date | Date | `.cast(DateType())` |
| String timestamp | Timestamp | `F.to_timestamp(col, 'yyyyMMddHHmmss')` |
| Null handling | Default | `F.coalesce(col, F.lit(default))` |

## Related

- [Bronze Ingestion](bronze-ingestion.md) - Previous layer
- [Gold Aggregation](gold-aggregation.md) - Next layer
- [Expectations Advanced](expectations-advanced.md) - Complex validations
