> **MCP Validated:** 2026-02-17

# Semantic Link (SemPy)

> **Purpose**: Using the SemPy library for data connectivity between Fabric notebooks and Power BI semantic models
> **Confidence**: 0.95

## Overview

Semantic Link (SemPy) is a Python library (`semantic-link`) that bridges Microsoft Fabric notebooks with Power BI semantic models. It provides `FabricDataFrame`, an enhanced pandas DataFrame with semantic metadata awareness. SemPy enables reading tables from semantic models, evaluating DAX measures, listing datasets, and performing data quality validations -- all from within Fabric notebooks without requiring separate Power BI connections.

## Installation

```python
# Pre-installed in Fabric notebooks; for local dev:
%pip install semantic-link
```

## Core Functions

### read_table

```python
import sempy.fabric as fabric

# Read a table from a Power BI semantic model into a FabricDataFrame
df = fabric.read_table(
    dataset="Sales Analytics",       # Semantic model name
    table_name="FactSales",          # Table within the model
    workspace="analytics-prod",      # Optional: defaults to current workspace
)

# FabricDataFrame supports all pandas operations
print(f"Rows: {len(df)}, Columns: {list(df.columns)}")
filtered = df[df["Region"] == "North America"]
```

### list_datasets

```python
import sempy.fabric as fabric

# List all semantic models in the current workspace
datasets = fabric.list_datasets()
print(datasets[["Dataset Name", "Dataset Id", "Configured By"]])

# List models in a specific workspace
datasets = fabric.list_datasets(workspace="analytics-prod")
```

### evaluate_measure

```python
import sempy.fabric as fabric

# Evaluate a DAX measure with optional filters
result = fabric.evaluate_measure(
    dataset="Sales Analytics",
    measure="Total Revenue",
    group_by_columns=["DimDate[Year]", "DimProduct[Category]"],
    filters={"DimDate[Year]": [2025, 2026]},
)
print(result)
# Returns a pandas DataFrame with measure values grouped by dimensions
```

### list_tables

```python
import sempy.fabric as fabric

# List all tables in a semantic model
tables = fabric.list_tables(dataset="Sales Analytics")
print(tables[["Name", "Type", "Description"]])
```

## FabricDataFrame

```python
import sempy.fabric as fabric
from sempy.fabric import FabricDataFrame

# FabricDataFrame extends pandas with semantic awareness
df = fabric.read_table("Sales Analytics", "FactSales")

# Access semantic metadata
print(type(df))  # <class 'sempy.fabric.FabricDataFrame'>

# All pandas operations work seamlessly
summary = df.groupby("Region")["Revenue"].sum().reset_index()

# Convert to standard pandas if needed
pdf = df.to_pandas()
```

## Integration with Pandas

```python
import sempy.fabric as fabric
import pandas as pd

# Read from semantic model
sales_df = fabric.read_table("Sales Analytics", "FactSales")
customers_df = fabric.read_table("Sales Analytics", "DimCustomer")

# Standard pandas merge
merged = pd.merge(
    sales_df,
    customers_df,
    left_on="CustomerKey",
    right_on="CustomerKey",
    how="inner",
)

# Write results back to a lakehouse delta table
spark_df = spark.createDataFrame(merged)
spark_df.write.format("delta").mode("overwrite").saveAsTable("enriched_sales")
```

## Quick Reference

| Function | Purpose | Returns |
|----------|---------|---------|
| `fabric.read_table()` | Read table from semantic model | FabricDataFrame |
| `fabric.list_datasets()` | List semantic models | DataFrame |
| `fabric.list_tables()` | List tables in a model | DataFrame |
| `fabric.evaluate_measure()` | Evaluate DAX measure | DataFrame |
| `fabric.list_relationships()` | List model relationships | DataFrame |
| `fabric.refresh_dataset()` | Trigger dataset refresh | None |

## Related

- [REST API Fundamentals](rest-api.md)
- [Fabric REST API v1](fabric-rest-api.md)
- [Python SDK Automation](../patterns/python-sdk-automation.md)
- [Power BI API](../patterns/power-bi-api.md)
