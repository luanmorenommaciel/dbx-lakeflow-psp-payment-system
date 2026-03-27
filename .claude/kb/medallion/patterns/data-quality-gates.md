# Data Quality Gates

> **Purpose**: Automated quality checks and quarantine patterns between Medallion layers
> **MCP Validated**: 2026-03-26

## When to Use

- Validating data between Bronze and Silver transitions
- Quarantining bad records to a separate table for investigation
- Enforcing data contracts with measurable SLA metrics
- Building audit trails for data quality over time

## Implementation

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, current_timestamp, count,
    sum as _sum, isnan, isnull
)
from dataclasses import dataclass
from typing import Optional

spark = SparkSession.builder.getOrCreate()


@dataclass
class QualityRule:
    """Definition of a single data quality check."""
    name: str
    column: str
    check: str           # "not_null", "positive", "regex", "range", "unique"
    threshold: float     # minimum pass rate (0.0 to 1.0)
    params: Optional[dict] = None


@dataclass
class QualityResult:
    """Result of a quality gate evaluation."""
    rule_name: str
    total_records: int
    passed_records: int
    failed_records: int
    pass_rate: float
    threshold: float
    passed: bool


def apply_quality_check(df: DataFrame, rule: QualityRule) -> DataFrame:
    """Add a boolean column indicating pass/fail for a quality rule."""
    c = col(rule.column)
    if rule.check == "not_null":
        condition = c.isNotNull() & (~isnan(c) if "double" in str(df.schema[rule.column].dataType) else lit(True))
    elif rule.check == "positive":
        condition = c > 0
    elif rule.check == "range":
        condition = (c >= rule.params["min"]) & (c <= rule.params["max"])
    elif rule.check == "regex":
        condition = c.rlike(rule.params["pattern"])
    elif rule.check == "not_empty":
        condition = c.isNotNull() & (c != "")
    else:
        condition = lit(True)

    return df.withColumn(f"_qc_{rule.name}", when(condition, True).otherwise(False))


def evaluate_quality_gate(df: DataFrame, rules: list[QualityRule]) -> tuple:
    """Run all quality rules and split into passed/quarantined DataFrames."""
    checked_df = df
    for rule in rules:
        checked_df = apply_quality_check(checked_df, rule)

    # All rules must pass for a record to proceed
    qc_columns = [f"_qc_{r.name}" for r in rules]
    all_passed = checked_df
    for qc_col in qc_columns:
        all_passed = all_passed.filter(col(qc_col) == True)

    # Records that failed any rule go to quarantine
    any_failed_condition = None
    for qc_col in qc_columns:
        cond = col(qc_col) == False
        any_failed_condition = cond if any_failed_condition is None else (any_failed_condition | cond)

    quarantined = checked_df.filter(any_failed_condition)

    # Drop QC columns from output
    clean_df = all_passed.drop(*qc_columns)
    quarantine_df = quarantined.withColumn("_quarantined_at", current_timestamp())

    return clean_df, quarantine_df
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `threshold` | `0.99` | Minimum pass rate (99%) to proceed |
| `quarantine_table` | `{layer}_quarantine.{table}` | Where failed records go |
| `halt_on_failure` | `false` | Stop pipeline if gate fails |
| `log_metrics` | `true` | Write quality metrics to audit table |

## Example Usage

```python
# Define quality rules for Bronze -> Silver transition
order_rules = [
    QualityRule("order_id_not_null", "order_id", "not_null", threshold=1.0),
    QualityRule("amount_positive", "amount", "positive", threshold=0.99),
    QualityRule("customer_not_empty", "customer_id", "not_empty", threshold=0.995),
    QualityRule("amount_range", "amount", "range", threshold=0.98,
                params={"min": 0.01, "max": 999999.99}),
]

# Run quality gate
bronze_df = spark.table("bronze_sales.raw_orders")
passed_df, quarantine_df = evaluate_quality_gate(bronze_df, order_rules)

# Write results
passed_df.write.format("delta").mode("append").saveAsTable("silver_sales.cleansed_orders")
quarantine_df.write.format("delta").mode("append").saveAsTable("quarantine.failed_orders")
```

## SQL Quality Gate

```sql
-- Quality gate as a SQL view for monitoring
CREATE OR REPLACE VIEW quality.bronze_orders_report AS
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_order_id,
    SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) AS valid_amount,
    SUM(CASE WHEN customer_id IS NOT NULL AND customer_id != '' THEN 1 ELSE 0 END) AS valid_customer,
    ROUND(SUM(CASE WHEN order_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS order_id_pct,
    ROUND(SUM(CASE WHEN amount > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS amount_pct,
    current_timestamp() AS _checked_at
FROM bronze_sales.raw_orders
WHERE _ingested_at > current_date() - INTERVAL 1 DAY;
```

## Quality Metrics Audit Table

```sql
CREATE TABLE IF NOT EXISTS quality.audit_log (
    check_id STRING,
    layer STRING,
    table_name STRING,
    rule_name STRING,
    total_records BIGINT,
    passed_records BIGINT,
    failed_records BIGINT,
    pass_rate DECIMAL(5,4),
    threshold DECIMAL(5,4),
    gate_passed BOOLEAN,
    checked_at TIMESTAMP
) USING DELTA;
```

## Shift-Left Quality: Bronze Validation (2025+ Best Practice)

Modern medallion implementations validate data earlier than traditional "quality only at Silver" approaches:

```python
# Bronze-level structural validation (not transformation)
bronze_structural_rules = [
    QualityRule("has_primary_key", "order_id", "not_null", threshold=1.0),
    QualityRule("parseable_json", "raw_payload", "not_empty", threshold=0.999),
]

# Silver-level business validation
silver_business_rules = [
    QualityRule("amount_positive", "amount", "positive", threshold=0.99),
    QualityRule("valid_email", "email", "regex", threshold=0.95,
                params={"pattern": r"^[^@]+@[^@]+\.[^@]+$"}),
]

# Gold-level aggregate validation
gold_aggregate_rules = [
    QualityRule("revenue_not_zero", "total_revenue", "positive", threshold=1.0),
    QualityRule("customer_count_range", "unique_customers", "range", threshold=1.0,
                params={"min": 1, "max": 10_000_000}),
]
```

## Quality Gate with Unity Catalog Data Quality Monitoring

```sql
-- Unity Catalog (Databricks): built-in data quality monitoring
ALTER TABLE silver_sales.cleansed_orders
SET TBLPROPERTIES ('quality.monitor.enabled' = 'true');

-- Define expectations (Databricks-native)
CREATE OR REPLACE EXPECTATION silver_orders_quality
ON TABLE silver_sales.cleansed_orders
EXPECT (order_id IS NOT NULL AND amount > 0)
WITH (violation_action = 'WARN');  -- or 'DROP', 'FAIL'
```

## See Also

- [Silver Layer](../concepts/silver-layer.md)
- [Layer Transitions](../patterns/layer-transitions.md)
- [Schema Evolution](../patterns/schema-evolution.md)
