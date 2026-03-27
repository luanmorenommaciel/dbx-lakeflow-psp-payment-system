# Spark Testing Patterns

> **Purpose**: PySpark DataFrame testing with session fixtures, schema validation, and transformation verification
> **MCP Validated**: 2026-02-17

## When to Use

- Testing PySpark DataFrame transformations
- Validating schema changes and column operations
- Testing aggregations, joins, and window functions
- Verifying data quality checks in Spark pipelines
- Testing UDFs (User Defined Functions)

## Implementation

```python
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)


# --- SparkSession Fixture ---

@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession for all tests."""
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("pytest-testing")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


# --- Schema Definition ---

INVOICE_SCHEMA = StructType([
    StructField("invoice_id", StringType(), False),
    StructField("vendor", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("status", StringType(), True),
])


# --- Sample Data Fixture ---

@pytest.fixture
def sample_invoices(spark):
    """Create a test DataFrame with known data."""
    data = [
        ("INV-001", "Acme Corp", 1500.00, "paid"),
        ("INV-002", "Beta Inc", 2500.00, "pending"),
        ("INV-003", "Acme Corp", 750.00, "paid"),
        ("INV-004", "Gamma LLC", 0.0, "cancelled"),
    ]
    return spark.createDataFrame(data, schema=INVOICE_SCHEMA)


# --- Transformation Under Test ---

def filter_active_invoices(df):
    """Business logic: filter to paid/pending with amount > 0."""
    return df.filter(
        (F.col("status").isin("paid", "pending")) &
        (F.col("amount") > 0)
    )


def aggregate_by_vendor(df):
    """Business logic: sum amounts per vendor."""
    return df.groupBy("vendor").agg(
        F.sum("amount").alias("total_amount"),
        F.count("*").alias("invoice_count"),
    )


# --- Tests ---

class TestSparkTransformations:
    def test_filter_active_invoices(self, sample_invoices):
        result = filter_active_invoices(sample_invoices)

        assert result.count() == 3
        statuses = [row.status for row in result.collect()]
        assert "cancelled" not in statuses

    def test_filter_excludes_zero_amount(self, sample_invoices):
        result = filter_active_invoices(sample_invoices)

        amounts = [row.amount for row in result.collect()]
        assert all(a > 0 for a in amounts)

    def test_aggregate_by_vendor(self, sample_invoices):
        active = filter_active_invoices(sample_invoices)
        result = aggregate_by_vendor(active)

        rows = {row.vendor: row for row in result.collect()}
        assert rows["Acme Corp"].total_amount == 2250.00
        assert rows["Acme Corp"].invoice_count == 2
        assert rows["Beta Inc"].total_amount == 2500.00

    def test_schema_after_transformation(self, sample_invoices):
        result = filter_active_invoices(sample_invoices)

        assert result.schema == INVOICE_SCHEMA

    def test_empty_dataframe(self, spark):
        empty_df = spark.createDataFrame([], schema=INVOICE_SCHEMA)

        result = filter_active_invoices(empty_df)

        assert result.count() == 0
        assert result.schema == INVOICE_SCHEMA
```

## DataFrame Assertion Helpers

```python
def assert_dataframe_equal(actual, expected, order_by=None):
    """Compare two DataFrames row by row."""
    if order_by:
        actual = actual.orderBy(order_by)
        expected = expected.orderBy(order_by)

    actual_rows = actual.collect()
    expected_rows = expected.collect()

    assert len(actual_rows) == len(expected_rows), (
        f"Row count mismatch: {len(actual_rows)} != {len(expected_rows)}"
    )
    for i, (a, e) in enumerate(zip(actual_rows, expected_rows)):
        assert a == e, f"Row {i} mismatch: {a} != {e}"


def assert_schema_contains(df, expected_fields):
    """Verify DataFrame contains expected columns with types."""
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    for name, dtype in expected_fields.items():
        assert name in actual_fields, f"Missing column: {name}"
        assert actual_fields[name] == dtype, (
            f"Column {name}: expected {dtype}, got {actual_fields[name]}"
        )
```

## Configuration

| Setting | Value | Purpose |
|---------|-------|---------|
| `spark.sql.shuffle.partitions` | `2` | Reduce partitions for speed |
| `spark.default.parallelism` | `2` | Limit parallelism in tests |
| `spark.ui.enabled` | `false` | Disable UI overhead |
| `master` | `local[2]` | Local mode with 2 cores |

## Example Usage

```python
# Run Spark tests with sufficient memory
# pytest tests/spark/ -v --timeout=60

# Marker-based selection
# pytest -m spark -v
```

## See Also

- [Fixtures](../concepts/fixtures.md)
- [Fixture Factories](../patterns/fixture-factories.md)
- [Integration Tests](../patterns/integration-tests.md)
