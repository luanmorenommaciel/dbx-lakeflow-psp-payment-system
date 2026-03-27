> **MCP Validated:** 2026-02-17

# Dynamic Data Masking

> **Purpose**: Implementing dynamic data masking in Fabric Warehouse and Lakehouse SQL endpoints

## When to Use

- Hiding sensitive PII columns (SSN, email, phone) from non-privileged users
- Enabling analyst access to production data without exposing raw sensitive values
- Complementing RLS (row-level) with column-level data obfuscation
- Meeting compliance requirements (GDPR, HIPAA) without duplicating data

## Implementation

```sql
-- Pattern 1: Create table with masked columns
CREATE TABLE dbo.customer_data (
    customer_id     INT NOT NULL,
    full_name       VARCHAR(200) NOT NULL,

    -- Default mask: replaces with "XXXX"
    ssn             VARCHAR(11)
                    MASKED WITH (FUNCTION = 'default()') NOT NULL,

    -- Email mask: shows first letter + "XXX@XXXX.com"
    email           VARCHAR(200)
                    MASKED WITH (FUNCTION = 'email()') NOT NULL,

    -- Partial mask: show first 3, mask middle, show last 2
    phone           VARCHAR(20)
                    MASKED WITH (FUNCTION = 'partial(3, "****", 2)') NOT NULL,

    -- Random mask: random number in range
    salary          DECIMAL(12,2)
                    MASKED WITH (FUNCTION = 'random(10000, 90000)') NOT NULL,

    region          VARCHAR(50) NOT NULL,
    created_at      DATETIME2 DEFAULT GETUTCDATE()
);

-- Pattern 2: Add masking to existing columns
ALTER TABLE dbo.customer_data
ALTER COLUMN full_name ADD MASKED WITH (FUNCTION = 'partial(2, "***", 1)');

-- Pattern 3: Grant UNMASK to specific users
-- Full unmask on all columns
GRANT UNMASK TO [admin@contoso.com];

-- Column-level unmask (granular)
GRANT UNMASK ON dbo.customer_data(email) TO [support@contoso.com];

-- Pattern 4: Remove masking
ALTER TABLE dbo.customer_data
ALTER COLUMN phone DROP MASKED;

-- Pattern 5: Verify masking behavior
-- As privileged user
EXECUTE AS USER = 'admin@contoso.com';
SELECT customer_id, ssn, email, phone, salary FROM dbo.customer_data;
-- Result: 12345, 123-45-6789, john@contoso.com, 555-123-4567, 85000.00
REVERT;

-- As non-privileged user
EXECUTE AS USER = 'analyst@contoso.com';
SELECT customer_id, ssn, email, phone, salary FROM dbo.customer_data;
-- Result: 12345, XXXX, jXXX@XXXX.com, 555****67, 42371.00
REVERT;
```

## Configuration

| Mask Function | Syntax | Output Example |
|---------------|--------|----------------|
| Default | `default()` | `XXXX` (string), `0` (number) |
| Email | `email()` | `jXXX@XXXX.com` |
| Partial | `partial(prefix, padding, suffix)` | `555****67` |
| Random | `random(start, end)` | Random int in range |

## Example Usage

```sql
-- Complete security layering: RLS + Masking + Column Security
-- Step 1: RLS (row filtering)
CREATE SECURITY POLICY dbo.RegionPolicy
    ADD FILTER PREDICATE dbo.fn_region_filter(region)
    ON dbo.customer_data
WITH (STATE = ON);

-- Step 2: Dynamic data masking (column obfuscation)
-- Already applied via CREATE TABLE above

-- Step 3: Column-level security (hide entire columns)
DENY SELECT ON dbo.customer_data(salary) TO [intern@contoso.com];

-- Step 4: Audit who has UNMASK permissions
SELECT
    dp.name AS principal_name,
    dp.type_desc AS principal_type,
    perm.permission_name,
    perm.state_desc
FROM sys.database_permissions perm
JOIN sys.database_principals dp ON perm.grantee_principal_id = dp.principal_id
WHERE perm.permission_name = 'UNMASK';

-- Query masked metadata
SELECT
    t.name AS table_name,
    c.name AS column_name,
    mc.masking_function
FROM sys.masked_columns mc
JOIN sys.columns c ON mc.object_id = c.object_id AND mc.column_id = c.column_id
JOIN sys.tables t ON c.object_id = t.object_id;
```

## Masking vs Encryption vs CLS

| Approach | Data at Rest | Data in Query | Use Case |
|----------|-------------|---------------|----------|
| Dynamic masking | Unchanged | Obfuscated per user | PII for analysts |
| Column-level security | N/A | Column hidden entirely | Salary, medical data |
| Encryption | Encrypted | Decrypted with key | Compliance (HIPAA) |
| RLS | Unchanged | Rows filtered | Multi-tenant, regional |

## See Also

- [RLS Security](../concepts/rls-security.md)
- [Warehouse Basics](../../04-data-warehouse/concepts/warehouse-basics.md)
- [T-SQL Patterns](../../04-data-warehouse/patterns/t-sql-patterns.md)
