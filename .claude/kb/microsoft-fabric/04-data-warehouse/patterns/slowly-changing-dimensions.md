> **MCP Validated:** 2026-02-17

# Slowly Changing Dimensions (SCD)

> **Purpose**: SCD Type 1 and Type 2 patterns with T-SQL MERGE and effective date tracking

## When to Use

- Dimension attributes change over time and you need to track history (Type 2)
- Dimension attributes change and you only need the latest value (Type 1)
- Building a star schema gold layer in Fabric Warehouse

## Overview

Slowly Changing Dimensions handle how dimension table rows are updated when source data changes. **SCD Type 1** overwrites the old value (no history). **SCD Type 2** preserves history by inserting a new row with effective dates. In Fabric Warehouse, MERGE statements are the primary mechanism for both patterns.

## SCD Type 1: Overwrite (No History)

Type 1 replaces the old attribute value. Use when historical values are not needed.

```sql
-- SCD Type 1: MERGE with overwrite on change
MERGE INTO dbo.dim_customer AS target
USING dbo.stg_customer AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND (
    target.customer_name <> source.customer_name OR
    target.email <> source.email OR
    target.segment <> source.segment
) THEN
    UPDATE SET
        target.customer_name = source.customer_name,
        target.email = source.email,
        target.segment = source.segment,
        target.updated_at = GETUTCDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_sk, customer_id, customer_name, email,
            segment, created_at, updated_at)
    VALUES (
        (SELECT ISNULL(MAX(customer_sk), 0) FROM dbo.dim_customer) +
            ROW_NUMBER() OVER (ORDER BY source.customer_id),
        source.customer_id, source.customer_name, source.email,
        source.segment, GETUTCDATE(), GETUTCDATE()
    );
```

## SCD Type 2: History Tracking (Effective Dates)

Type 2 inserts a new row when attributes change, preserving full history. Requires surrogate keys, `is_current` flag, and `effective_start`/`effective_end` columns.

### Table Structure

```sql
CREATE TABLE dbo.dim_product (
    product_sk      BIGINT NOT NULL,        -- Surrogate key
    product_id      VARCHAR(50) NOT NULL,   -- Business key
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    list_price      DECIMAL(10,2) NOT NULL,
    is_current      BIT NOT NULL DEFAULT 1,
    effective_start DATE NOT NULL,
    effective_end   DATE NOT NULL DEFAULT '9999-12-31',
    row_hash        VARCHAR(64) NOT NULL    -- Change detection
);
```

### Stored Procedure Pattern

```sql
CREATE PROCEDURE dbo.usp_scd2_dim_product
AS
BEGIN
    -- Step 1: Expire changed records (close effective_end)
    UPDATE dbo.dim_product
    SET is_current = 0, effective_end = CAST(GETUTCDATE() AS DATE)
    WHERE is_current = 1
    AND product_id IN (
        SELECT s.product_id FROM dbo.stg_product s
        JOIN dbo.dim_product d ON s.product_id = d.product_id AND d.is_current = 1
        WHERE s.row_hash <> d.row_hash
    );

    -- Step 2: Insert new versions for changed + new records
    DECLARE @max_sk BIGINT = ISNULL((SELECT MAX(product_sk) FROM dbo.dim_product), 0);

    INSERT INTO dbo.dim_product
        (product_sk, product_id, product_name, category,
         list_price, is_current, effective_start, effective_end, row_hash)
    SELECT
        @max_sk + ROW_NUMBER() OVER (ORDER BY s.product_id),
        s.product_id, s.product_name, s.category, s.list_price,
        1, CAST(GETUTCDATE() AS DATE), '9999-12-31', s.row_hash
    FROM dbo.stg_product s
    WHERE s.row_hash <> (
        SELECT TOP 1 d.row_hash FROM dbo.dim_product d
        WHERE d.product_id = s.product_id AND d.is_current = 1
    )
    OR NOT EXISTS (
        SELECT 1 FROM dbo.dim_product d WHERE d.product_id = s.product_id
    );
END;
```

### Querying SCD Type 2

```sql
-- Current version
SELECT * FROM dbo.dim_product WHERE is_current = 1;

-- Point-in-time lookup
SELECT * FROM dbo.dim_product
WHERE product_id = 'PROD-001'
  AND '2026-01-15' BETWEEN effective_start AND effective_end;

-- Full history
SELECT product_name, list_price, effective_start, effective_end, is_current
FROM dbo.dim_product WHERE product_id = 'PROD-001'
ORDER BY effective_start;
```

## Comparison

| Aspect | SCD Type 1 | SCD Type 2 |
|--------|-----------|-----------|
| History preserved | No | Yes |
| Table growth | Stable | Grows over time |
| Complexity | Low (MERGE) | Medium (expire + insert) |
| Audit trail | No | Full history with dates |
| Fact join | Current key | Surrogate key per version |
| Use case | Corrections, non-critical | Price/segment changes |

## Configuration

| Setting | Type 1 | Type 2 |
|---------|--------|--------|
| Surrogate key | Optional | Required |
| is_current flag | Not needed | Required |
| effective_start/end | Not needed | Required (`'9999-12-31'`) |
| row_hash | Optional | Recommended |

## Common Mistakes

### Wrong

```sql
-- Using UPDATE for SCD Type 2 (destroys history)
UPDATE dbo.dim_product SET product_name = 'New Name' WHERE product_id = 'PROD-001';
```

### Correct

```sql
-- Expire old row, then INSERT new version via stored procedure
EXEC dbo.usp_scd2_dim_product;
```

## See Also

- [T-SQL Patterns](t-sql-patterns.md)
- [Star Schema](star-schema.md)
- [Warehouse Basics](../concepts/warehouse-basics.md)
- [Medallion in Fabric](../../03-architecture-patterns/patterns/medallion-fabric.md)
