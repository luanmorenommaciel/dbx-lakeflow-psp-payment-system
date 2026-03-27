> **MCP Validated:** 2026-02-17

# Star Schema in Fabric Warehouse

> **Purpose**: Implementing star schema with fact tables, dimension tables, and surrogate keys

## When to Use

- Building a gold layer in Fabric Warehouse for Power BI consumption
- Designing a semantic model optimized for Direct Lake mode
- Creating a data mart for departmental analytics
- Migrating an existing star schema from SQL Server or Azure Synapse

## Overview

Star schema is the recommended data modeling pattern for Fabric Warehouse gold layers and Direct Lake semantic models. It consists of a central fact table (events/transactions) surrounded by dimension tables (descriptive attributes). Fabric's automatic V-Order optimization and Direct Lake mode are specifically designed for star schema layouts.

## Implementation

### Dimension Tables

```sql
-- Date dimension (essential for any star schema)
CREATE TABLE dbo.dim_date (
    date_key        INT NOT NULL,           -- YYYYMMDD format
    full_date       DATE NOT NULL,
    day_of_week     TINYINT NOT NULL,
    day_name        VARCHAR(10) NOT NULL,
    day_of_month    TINYINT NOT NULL,
    day_of_year     SMALLINT NOT NULL,
    week_of_year    TINYINT NOT NULL,
    month_number    TINYINT NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         TINYINT NOT NULL,
    year            SMALLINT NOT NULL,
    is_weekend      BIT NOT NULL,
    is_holiday      BIT NOT NULL,
    fiscal_quarter  TINYINT NOT NULL,
    fiscal_year     SMALLINT NOT NULL
);

-- Customer dimension with surrogate key
CREATE TABLE dbo.dim_customer (
    customer_sk     BIGINT NOT NULL,        -- Surrogate key
    customer_id     VARCHAR(50) NOT NULL,   -- Business/natural key
    customer_name   VARCHAR(200) NOT NULL,
    email           VARCHAR(200),
    segment         VARCHAR(50) NOT NULL,
    region          VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    country         VARCHAR(100) NOT NULL,
    created_date    DATE NOT NULL,
    is_current      BIT NOT NULL DEFAULT 1,
    effective_start DATE NOT NULL,
    effective_end   DATE NOT NULL DEFAULT '9999-12-31'
);

-- Product dimension with surrogate key
CREATE TABLE dbo.dim_product (
    product_sk      BIGINT NOT NULL,        -- Surrogate key
    product_id      VARCHAR(50) NOT NULL,   -- Business key
    product_name    VARCHAR(200) NOT NULL,
    category        VARCHAR(100) NOT NULL,
    subcategory     VARCHAR(100),
    brand           VARCHAR(100),
    unit_cost       DECIMAL(10,2),
    list_price      DECIMAL(10,2),
    is_current      BIT NOT NULL DEFAULT 1,
    effective_start DATE NOT NULL,
    effective_end   DATE NOT NULL DEFAULT '9999-12-31'
);
```

### Fact Table

```sql
-- Sales fact table referencing dimension surrogate keys
CREATE TABLE dbo.fact_sales (
    sale_id         BIGINT NOT NULL,
    date_key        INT NOT NULL,           -- FK to dim_date
    customer_sk     BIGINT NOT NULL,        -- FK to dim_customer
    product_sk      BIGINT NOT NULL,        -- FK to dim_product
    quantity        INT NOT NULL,
    unit_price      DECIMAL(10,2) NOT NULL,
    discount_pct    DECIMAL(5,2) DEFAULT 0,
    total_amount    DECIMAL(18,2) NOT NULL,
    cost_amount     DECIMAL(18,2),
    profit_amount   AS (total_amount - ISNULL(cost_amount, 0)),
    order_number    VARCHAR(50) NOT NULL,
    loaded_at       DATETIME2 DEFAULT GETUTCDATE()
);
```

### Surrogate Key Generation

```sql
-- Fabric Warehouse does not support IDENTITY or SEQUENCE.
-- Use ROW_NUMBER() to generate surrogate keys.

-- Pattern: Generate surrogate keys during dimension load
CREATE PROCEDURE dbo.usp_load_dim_customer
AS
BEGIN
    -- Get current max surrogate key
    DECLARE @max_sk BIGINT = ISNULL(
        (SELECT MAX(customer_sk) FROM dbo.dim_customer), 0
    );

    -- Insert new customers with generated surrogate keys
    INSERT INTO dbo.dim_customer
        (customer_sk, customer_id, customer_name, email,
         segment, region, city, country, created_date,
         is_current, effective_start, effective_end)
    SELECT
        @max_sk + ROW_NUMBER() OVER (ORDER BY s.customer_id),
        s.customer_id,
        s.customer_name,
        s.email,
        s.segment,
        s.region,
        s.city,
        s.country,
        CAST(GETUTCDATE() AS DATE),
        1,
        CAST(GETUTCDATE() AS DATE),
        '9999-12-31'
    FROM dbo.stg_customer s
    WHERE NOT EXISTS (
        SELECT 1 FROM dbo.dim_customer d
        WHERE d.customer_id = s.customer_id
        AND d.is_current = 1
    );
END;
```

### Reporting Views

```sql
-- Star schema reporting view
CREATE VIEW dbo.vw_sales_analysis AS
SELECT
    d.full_date,
    d.year,
    d.quarter,
    d.month_name,
    c.customer_name,
    c.segment,
    c.region,
    p.product_name,
    p.category,
    p.brand,
    f.quantity,
    f.unit_price,
    f.total_amount,
    f.profit_amount
FROM dbo.fact_sales f
JOIN dbo.dim_date d ON f.date_key = d.date_key
JOIN dbo.dim_customer c ON f.customer_sk = c.customer_sk
JOIN dbo.dim_product p ON f.product_sk = p.product_sk
WHERE c.is_current = 1
  AND p.is_current = 1;
```

## Configuration

| Setting | Recommendation | Rationale |
|---------|----------------|-----------|
| Surrogate keys | BIGINT via ROW_NUMBER() | No IDENTITY/SEQUENCE in Fabric |
| Date dimension | Pre-populate 20+ years | Covers historical and future |
| Fact grain | One row per transaction | Lowest grain for flexibility |
| Computed columns | Use `AS` expression | profit_amount example above |
| V-Order | Automatic in Warehouse | No manual action needed |

## Design Guidelines

| Guideline | Description |
|-----------|-------------|
| Keep facts narrow | Only keys + measures + degenerate dims |
| Keep dimensions wide | All descriptive attributes in dimension |
| Use integer keys for joins | Surrogate keys improve join performance |
| Avoid snowflaking | Flatten hierarchies into dimension tables |
| One fact table per process | Sales, inventory, and shipments as separate facts |

## See Also

- [Warehouse Basics](../concepts/warehouse-basics.md)
- [T-SQL Patterns](t-sql-patterns.md)
- [Slowly Changing Dimensions](slowly-changing-dimensions.md)
- [Direct Lake](../concepts/direct-lake.md)
