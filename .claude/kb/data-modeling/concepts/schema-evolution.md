# Schema Evolution

> **Purpose**: Iceberg/Delta schema evolution, Avro/Protobuf rules, backward/forward compatibility
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Schema evolution handles column additions, type changes, and renames without breaking downstream consumers. Open table formats (Iceberg, Delta) support evolution natively. The key principle: additive changes are safe; removals and type narrowing are breaking.

## The Concept

```sql
-- Iceberg: Full schema evolution support
ALTER TABLE catalog.db.orders ADD COLUMN loyalty_tier VARCHAR;
ALTER TABLE catalog.db.orders RENAME COLUMN loyalty_tier TO customer_tier;
ALTER TABLE catalog.db.orders ALTER COLUMN quantity TYPE BIGINT;  -- widening: safe
ALTER TABLE catalog.db.orders DROP COLUMN deprecated_flag;

-- Delta Lake: Schema evolution via mergeSchema
MERGE INTO orders AS target
USING updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
-- With: .option("mergeSchema", "true") on the writer

-- Avro: Backward-compatible evolution
-- Safe: add field with default, remove field with default
-- Breaking: add field without default, change type, rename field
```

## Quick Reference

| Change Type | Iceberg v3 | Delta 4.x | Avro | Breaking? |
|-------------|---------|-------|------|-----------|
| Add column (nullable) | `ALTER TABLE ADD` | `mergeSchema` | Add with default | No |
| Add column (NOT NULL) | Requires default | Requires default | No default = breaking | Yes |
| Drop column | `ALTER TABLE DROP` | Column mapping mode (name) | Remove with default | Depends |
| Rename column | `ALTER TABLE RENAME` | Column mapping mode (name) | N/A (use aliases) | Yes (for consumers) |
| Widen type (INT->BIGINT) | `ALTER TABLE ALTER` | **Type widening** (4.0+, no data rewrite) | Promotion rules | No |
| Narrow type (BIGINT->INT) | Not allowed | Not allowed | Not allowed | Yes |
| Reorder columns | Supported | Not applicable | Positional in Avro | No |
| **Variant type** | **v3 native** | **4.0+ native** | N/A | No (additive) |
| **Geospatial types** | **v3 (geometry/geography)** | Not native | N/A | No (additive) |
| **Default values** | **v3 native** | Supported | Supported | No |

### Delta Lake 4.0+ Type Widening

```sql
-- Enable type widening (no data rewrite required)
ALTER TABLE orders SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true');

-- Supported widenings:
-- BYTE -> SHORT, INT, BIGINT, DECIMAL, DOUBLE
-- SHORT -> INT, BIGINT, DECIMAL, DOUBLE
-- INT -> BIGINT, DECIMAL, DOUBLE
-- BIGINT -> DECIMAL
-- FLOAT -> DOUBLE
-- DECIMAL -> DECIMAL (greater precision/scale)
-- DATE -> TIMESTAMP_NTZ
ALTER TABLE orders ALTER COLUMN quantity TYPE BIGINT;  -- INT -> BIGINT, no rewrite
```

## Common Mistakes

### Wrong

```sql
-- Breaking change: dropping a column consumers depend on
ALTER TABLE orders DROP COLUMN customer_email;
-- No deprecation period, no communication, consumers break immediately
```

### Correct

```sql
-- Safe evolution: deprecation lifecycle
-- Step 1: Add new column
ALTER TABLE orders ADD COLUMN customer_email_v2 VARCHAR;

-- Step 2: Populate new column, mark old as deprecated (comment/tag)
COMMENT ON COLUMN orders.customer_email IS 'DEPRECATED: use customer_email_v2. Removal: 2026-06-01';

-- Step 3: Migrate consumers (30-day window)
-- Step 4: Drop old column only after all consumers migrated
ALTER TABLE orders DROP COLUMN customer_email;
```

## Related

- [scd-types](scd-types.md)
- [schema-migration pattern](../patterns/schema-migration.md)
- [data-contracts](../../data-quality/concepts/data-contracts.md)
