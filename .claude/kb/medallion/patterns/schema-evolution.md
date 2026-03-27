# Schema Evolution

> **Purpose**: Handling schema changes across Medallion layers with Delta Lake / Iceberg evolution features
> **MCP Validated**: 2026-03-26

## When to Use

- Source systems add new columns that need to propagate through layers
- Data types change or need widening (e.g., INT to BIGINT)
- Columns are renamed or removed in upstream sources
- Migrating from one schema version to another without downtime

## Implementation

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()


def configure_schema_evolution(table_name: str):
    """Enable schema evolution features on a Delta table."""
    spark.sql(f"""
        ALTER TABLE {table_name} SET TBLPROPERTIES (
            'delta.columnMapping.mode' = 'name',
            'delta.minReaderVersion' = '2',
            'delta.minWriterVersion' = '5'
        )
    """)


def bronze_schema_evolution(source_path: str, bronze_table: str):
    """Bronze: Accept any schema changes from source automatically."""
    raw_df = spark.read.option("mergeSchema", "true").json(source_path)

    (
        raw_df
        .withColumn("_ingested_at", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")       # auto-add new columns
        .saveAsTable(bronze_table)
    )


def silver_schema_migration(silver_table: str, migrations: list[dict]):
    """Apply explicit schema migrations to Silver tables."""
    for migration in migrations:
        action = migration["action"]

        if action == "add_column":
            spark.sql(f"""
                ALTER TABLE {silver_table}
                ADD COLUMN {migration['name']} {migration['type']}
                {f"DEFAULT {migration['default']}" if 'default' in migration else ''}
            """)

        elif action == "rename_column":
            spark.sql(f"""
                ALTER TABLE {silver_table}
                RENAME COLUMN {migration['old_name']} TO {migration['new_name']}
            """)

        elif action == "change_type":
            spark.sql(f"""
                ALTER TABLE {silver_table}
                ALTER COLUMN {migration['name']} TYPE {migration['new_type']}
            """)

        elif action == "drop_column":
            spark.sql(f"""
                ALTER TABLE {silver_table}
                DROP COLUMN {migration['name']}
            """)


# Usage
migrations = [
    {"action": "add_column", "name": "loyalty_tier", "type": "STRING", "default": "'standard'"},
    {"action": "rename_column", "old_name": "amt", "new_name": "amount"},
    {"action": "change_type", "name": "amount", "new_type": "DECIMAL(12,2)"},
]
silver_schema_migration("silver_sales.cleansed_orders", migrations)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `delta.columnMapping.mode` | `none` | Set to `name` to enable rename/drop |
| `mergeSchema` | `false` | Set to `true` to auto-add columns on write |
| `overwriteSchema` | `false` | Set to `true` to replace schema entirely |
| `delta.minReaderVersion` | `1` | Set to `2` for column mapping |
| `delta.minWriterVersion` | `5` | Set to `5` for column mapping |

## Schema Evolution by Layer

| Capability | Bronze | Silver | Gold |
|-----------|--------|--------|------|
| Auto-add columns | `mergeSchema=true` | Explicit migration | Schema follows Silver |
| Rename columns | Not needed | `ALTER COLUMN RENAME` | Rebuild from Silver |
| Drop columns | Never | With `columnMapping=name` (Delta) or native (Iceberg) | Rebuild from Silver |
| Type widening | Not needed | `ALTER COLUMN TYPE` or type widening (Delta 4.0+) | Rebuild from Silver |
| Full schema replace | Never | Rare, explicit | `overwriteSchema=true` |
| Variant type (semi-structured) | Store as Variant (Delta 4.0+ / Iceberg v3) | Extract typed columns from Variant | N/A (use typed columns) |

### Iceberg Schema Evolution at Each Layer

```sql
-- Bronze (Iceberg): auto-evolve schema on write
-- Iceberg handles schema evolution natively via metadata
ALTER TABLE catalog.bronze.raw_events ADD COLUMN new_field STRING;

-- Silver (Iceberg): explicit evolution with full support
ALTER TABLE catalog.silver.orders RENAME COLUMN amt TO amount;
ALTER TABLE catalog.silver.orders ALTER COLUMN quantity TYPE BIGINT;
ALTER TABLE catalog.silver.orders DROP COLUMN deprecated_flag;

-- Gold (Iceberg): rebuild from Silver
CREATE OR REPLACE TABLE catalog.gold.agg_revenue AS SELECT ...;
```

## SQL Schema Migration Pattern

```sql
-- Step 1: Enable column mapping (one-time setup)
ALTER TABLE silver_sales.cleansed_orders SET TBLPROPERTIES (
    'delta.columnMapping.mode' = 'name',
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5'
);

-- Step 2: Add new column with default
ALTER TABLE silver_sales.cleansed_orders
ADD COLUMN loyalty_tier STRING DEFAULT 'standard';

-- Step 3: Rename column
ALTER TABLE silver_sales.cleansed_orders
RENAME COLUMN amt TO amount;

-- Step 4: Widen data type
ALTER TABLE silver_sales.cleansed_orders
ALTER COLUMN amount TYPE DECIMAL(12,2);

-- Step 5: Drop deprecated column
ALTER TABLE silver_sales.cleansed_orders
DROP COLUMN legacy_field;

-- Step 6: Backfill new column
UPDATE silver_sales.cleansed_orders
SET loyalty_tier = CASE
    WHEN lifetime_revenue > 10000 THEN 'gold'
    WHEN lifetime_revenue > 5000 THEN 'silver'
    ELSE 'standard'
END
WHERE loyalty_tier = 'standard';
```

## Version Tracking

```sql
-- Track schema versions for auditability
CREATE TABLE IF NOT EXISTS meta.schema_versions (
    table_name STRING,
    version INT,
    change_description STRING,
    columns_added ARRAY<STRING>,
    columns_removed ARRAY<STRING>,
    columns_renamed MAP<STRING, STRING>,
    applied_at TIMESTAMP,
    applied_by STRING
) USING DELTA;

-- Log migration
INSERT INTO meta.schema_versions VALUES (
    'silver_sales.cleansed_orders', 3,
    'Added loyalty_tier column, renamed amt to amount',
    ARRAY('loyalty_tier'), ARRAY(), MAP('amt', 'amount'),
    current_timestamp(), 'pipeline_v2.1'
);
```

## Example Usage

```python
# Full schema evolution workflow
configure_schema_evolution("silver_sales.cleansed_orders")

# Bronze auto-evolves
bronze_schema_evolution("/mnt/landing/orders/", "bronze_sales.raw_orders")

# Silver explicit migration
silver_schema_migration("silver_sales.cleansed_orders", [
    {"action": "add_column", "name": "channel", "type": "STRING", "default": "'web'"},
])
```

## See Also

- [Bronze Layer](../concepts/bronze-layer.md)
- [Silver Layer](../concepts/silver-layer.md)
- [Incremental Loading](../patterns/incremental-loading.md)
