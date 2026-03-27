# Schema Migration

> **Purpose**: Backward-compatible migrations, blue-green deploys, column deprecation, testing
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Schema migrations in data warehouses must be backward-compatible to avoid breaking downstream consumers. The golden rule: additive changes are safe; removals require a deprecation lifecycle. Blue-green deployments allow testing new schemas before cutover.

## The Pattern

```sql
-- ============================================================
-- Safe Migration: Add column + deprecate old column
-- ============================================================

-- Step 1: Additive change (always safe)
ALTER TABLE orders ADD COLUMN customer_tier VARCHAR(20);

-- Step 2: Backfill new column
UPDATE orders
SET customer_tier = CASE
    WHEN total_amount > 10000 THEN 'platinum'
    WHEN total_amount > 5000  THEN 'gold'
    WHEN total_amount > 1000  THEN 'silver'
    ELSE 'bronze'
END;

-- Step 3: Mark old column as deprecated (metadata/comment)
COMMENT ON COLUMN orders.customer_level IS
    'DEPRECATED 2026-03-26: Use customer_tier instead. Removal: 2026-06-01';

-- Step 4: Create view for backward compatibility during transition
CREATE OR REPLACE VIEW orders_compat AS
SELECT
    *,
    customer_tier AS customer_level  -- alias for old name
FROM orders;

-- Step 5: After migration window (30+ days), drop old column
-- Only after confirming no consumers reference it
ALTER TABLE orders DROP COLUMN customer_level;

-- ============================================================
-- Blue-Green Schema Deploy
-- ============================================================

-- Green (new schema): create alongside existing
CREATE TABLE orders_v2 AS
SELECT
    order_id,
    customer_id,
    customer_tier,      -- new column
    order_date,
    total_amount
FROM orders;

-- Validation: compare row counts and checksums
SELECT
    (SELECT COUNT(*) FROM orders) AS blue_count,
    (SELECT COUNT(*) FROM orders_v2) AS green_count,
    (SELECT SUM(total_amount) FROM orders) AS blue_sum,
    (SELECT SUM(total_amount) FROM orders_v2) AS green_sum;

-- Cutover: rename (atomic swap where supported)
ALTER TABLE orders RENAME TO orders_deprecated;
ALTER TABLE orders_v2 RENAME TO orders;

-- Rollback if needed
ALTER TABLE orders RENAME TO orders_v2;
ALTER TABLE orders_deprecated RENAME TO orders;
```

## Quick Reference

| Change | Risk | Delta 4.x Strategy | Iceberg v3 Strategy |
|--------|------|-------------------|-------------------|
| Add nullable column | None | `mergeSchema` or `ALTER ADD` | `ALTER TABLE ADD` |
| Add NOT NULL column | Low | Add nullable -> backfill -> constraint | Add with default value (v3) |
| Rename column | Medium | Column mapping mode (name) | `ALTER TABLE RENAME` |
| Change type (widen) | Low | **Type widening** (no rewrite, 4.0+) | `ALTER TABLE ALTER TYPE` |
| Change type (narrow) | High | New column -> CAST -> validate -> swap | Not allowed (by design) |
| Drop column | High | Column mapping + deprecation lifecycle | `ALTER TABLE DROP` + deprecation |
| Drop table | Critical | Rename to _deprecated -> wait -> drop | Same pattern |
| Add Variant column | None | Native (4.0+) | Native (v3) |

## Common Mistakes

### Wrong

```sql
-- Immediate drop without deprecation — breaks all downstream consumers
ALTER TABLE orders DROP COLUMN customer_level;
```

### Correct

```sql
-- Deprecation lifecycle: mark → communicate → wait → validate → drop
COMMENT ON COLUMN orders.customer_level IS 'DEPRECATED 2026-03-26. Removal: 2026-06-01';
-- Wait 30+ days, verify no consumers, then drop
```

## Related

- [schema-evolution](../concepts/schema-evolution.md)
- [data-contracts](../../data-quality/concepts/data-contracts.md)
- [scd-types](../concepts/scd-types.md)
