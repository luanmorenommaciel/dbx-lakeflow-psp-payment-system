> **MCP Validated:** 2026-02-17

# T-SQL Patterns

> **Purpose**: Production T-SQL patterns for Fabric Data Warehouse -- MERGE, SCD, stored procedures

## When to Use

- Implementing slowly changing dimensions (SCD Type 1 and Type 2) in Fabric Warehouse
- Building stored procedures for repeatable data transformations
- Performing UPSERT operations with MERGE statements
- Creating cross-database queries between Warehouse and Lakehouse

## Implementation

```sql
-- Pattern 1: MERGE (Upsert) for SCD Type 1
MERGE INTO dbo.dim_customer AS target
USING dbo.stg_customer AS source
ON target.customer_id = source.customer_id
WHEN MATCHED AND (
    target.customer_name <> source.customer_name OR
    target.email <> source.email OR
    target.region <> source.region
) THEN
    UPDATE SET
        target.customer_name = source.customer_name,
        target.email = source.email,
        target.region = source.region,
        target.updated_at = GETUTCDATE()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (customer_id, customer_name, email, region, created_at, updated_at)
    VALUES (source.customer_id, source.customer_name, source.email,
            source.region, GETUTCDATE(), GETUTCDATE());

-- Pattern 2: SCD Type 2 with effective dates
CREATE PROCEDURE dbo.usp_load_dim_product_scd2
AS
BEGIN
    -- Close existing records that have changed
    UPDATE dbo.dim_product
    SET
        is_current = 0,
        effective_end = GETUTCDATE()
    WHERE is_current = 1
    AND product_id IN (
        SELECT s.product_id
        FROM dbo.stg_product s
        JOIN dbo.dim_product d ON s.product_id = d.product_id
        WHERE d.is_current = 1
        AND (d.product_name <> s.product_name OR d.price <> s.price)
    );

    -- Insert new versions for changed records
    INSERT INTO dbo.dim_product
        (product_id, product_name, category, price, is_current, effective_start, effective_end)
    SELECT
        s.product_id, s.product_name, s.category, s.price,
        1, GETUTCDATE(), '9999-12-31'
    FROM dbo.stg_product s
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.dim_product d
        WHERE d.product_id = s.product_id
        AND d.is_current = 1
        AND d.product_name = s.product_name
        AND d.price = s.price
    );
END;

-- Pattern 3: Cross-database query (Lakehouse to Warehouse)
SELECT
    w.customer_name,
    w.region,
    l.total_orders,
    l.total_revenue
FROM dbo.dim_customer w
JOIN silver_lakehouse.dbo.silver_order_summary l
    ON w.customer_id = l.customer_id
WHERE l.total_revenue > 10000;

-- Pattern 4: Stored procedure with error handling
CREATE PROCEDURE dbo.usp_refresh_gold_layer
AS
BEGIN
    BEGIN TRY
        -- Truncate and reload gold summary
        DELETE FROM dbo.gold_sales_summary;

        INSERT INTO dbo.gold_sales_summary
            (sale_month, region, category, revenue, order_count)
        SELECT
            FORMAT(order_date, 'yyyy-MM'),
            region,
            category,
            SUM(amount),
            COUNT(*)
        FROM dbo.fact_sales f
        JOIN dbo.dim_product p ON f.product_id = p.product_id
        GROUP BY FORMAT(order_date, 'yyyy-MM'), region, category;

    END TRY
    BEGIN CATCH
        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        THROW 50001, @ErrorMsg, 1;
    END CATCH
END;
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Statement timeout | 30 min | Max query execution time |
| Max row count | Unlimited | No artificial row limits |
| Cross-DB queries | Enabled | Three-part naming: `db.schema.table` |
| Transactions | Supported | Explicit BEGIN/COMMIT/ROLLBACK |
| Temp tables | Supported | Session-scoped `#temp` tables |

## Example Usage

```sql
-- Execute stored procedure from a pipeline
EXEC dbo.usp_load_dim_product_scd2;
EXEC dbo.usp_refresh_gold_layer;

-- Verify results
SELECT TOP 10 * FROM dbo.dim_product WHERE is_current = 1 ORDER BY effective_start DESC;
SELECT * FROM dbo.gold_sales_summary WHERE sale_month = FORMAT(GETDATE(), 'yyyy-MM');
```

## Feature Support Matrix

| T-SQL Feature | Supported | Notes |
|---------------|-----------|-------|
| MERGE | Yes | Full UPSERT support |
| CTEs | Yes | WITH clause |
| Window functions | Yes | ROW_NUMBER, RANK, etc. |
| Stored procedures | Yes | No CLR support |
| Temp tables | Yes | Session-scoped |
| Cross-DB joins | Yes | Three-part naming |
| OPENROWSET | No | Use shortcuts instead |
| Triggers | No | Use pipeline orchestration |
| Indexes | No | Auto-optimized with V-Order |
| Sequences | No | Use IDENTITY or manual |

## See Also

- [Warehouse Basics](../concepts/warehouse-basics.md)
- [RLS Security](../../06-governance-security/concepts/rls-security.md)
- [Medallion in Fabric](../../03-architecture-patterns/patterns/medallion-fabric.md)
