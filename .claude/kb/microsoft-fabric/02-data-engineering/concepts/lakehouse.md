> **MCP Validated:** 2026-03-26

# Lakehouse

> **Purpose**: Lakehouse architecture and OneLake fundamentals in Microsoft Fabric
> **Confidence**: 0.95

## Overview

The Fabric Lakehouse is a data architecture platform combining the best of data lakes and data warehouses. Built on OneLake (the unified logical data lake), it stores data in Delta Parquet format with full ACID transactions. Every Lakehouse automatically gets a SQL analytics endpoint for T-SQL queries, a default semantic model for Power BI, and now an Eventhouse endpoint for real-time KQL analytics (GA Nov 2025).

**Key 2025 updates:**
- **Eventhouse endpoint for Lakehouse** -- KQL-powered real-time analytics over Lakehouse Delta tables
- **OneLake shortcuts expanded** -- Azure Blob Storage (May 2025), SharePoint/OneDrive (Dec 2025), Key Vault support
- **Govern in OneLake** (preview) -- tenant-level governance controls for OneLake data

## The Pattern

```python
# PySpark notebook -- Lakehouse data ingestion
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

# Spark session is pre-configured in Fabric notebooks
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/raw/sales_2024.csv")

# Add metadata columns
df_enriched = df \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_system", lit("erp_sales"))

# Write as Delta table (managed by Lakehouse)
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze_sales")

# Optimize for read performance
spark.sql("OPTIMIZE bronze_sales VORDER")
```

## Quick Reference

| Feature | Lakehouse | Warehouse |
|---------|-----------|-----------|
| Storage format | Delta Parquet | Delta Parquet |
| Query engine | Spark + SQL endpoint | T-SQL (full DML) |
| Write access | Spark, Dataflows, Copy | T-SQL INSERT/UPDATE |
| V-Order optimization | Yes | Automatic |
| Shortcuts | Yes (cross-cloud) | No |
| Schema enforcement | Schema-on-read | Schema-on-write |

## Common Mistakes

### Wrong

```python
# Writing non-Delta format to Lakehouse Tables
df.write.format("parquet").save("Tables/my_table")
```

### Correct

```python
# Always use Delta format for managed tables
df.write.format("delta").mode("append").saveAsTable("my_table")
```

## Key Components

| Component | Description |
|-----------|-------------|
| **Tables/** | Managed Delta tables (auto-registered in metastore) |
| **Files/** | Unstructured files (CSV, JSON, images, etc.) |
| **SQL endpoint** | Read-only T-SQL access to Delta tables |
| **Eventhouse endpoint** | KQL real-time analytics over Delta tables (GA Nov 2025) |
| **Semantic model** | Auto-generated Power BI dataset |
| **Shortcuts** | Zero-copy references to ADLS, S3, GCS, Azure Blob, SharePoint, OneDrive |

## Related

- [Workload Selection](../../03-architecture-patterns/concepts/workload-selection.md)
- [Copy Activity](../patterns/copy-activity.md)
- [Medallion in Fabric](../../03-architecture-patterns/patterns/medallion-fabric.md)
