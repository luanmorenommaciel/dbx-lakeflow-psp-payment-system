> **MCP Validated:** 2026-02-17

# Row-Level Security (RLS)

> **Purpose**: Implementing row-level security in Fabric Warehouse and Lakehouse SQL endpoints
> **Confidence**: 0.95

## Overview

Row-Level Security (RLS) in Microsoft Fabric restricts data access at the row level based on the user's identity or role. It uses predicate-based security with inline table-valued functions and security policies. RLS is supported in the Fabric Warehouse (full support) and the Lakehouse SQL analytics endpoint (read-only enforcement). Filter predicates silently filter rows -- users see only their authorized data.

## The Pattern

```sql
-- Step 1: Create a mapping table for user-region access
CREATE TABLE dbo.security_mapping (
    user_email  VARCHAR(200) NOT NULL,
    region      VARCHAR(50) NOT NULL
);

INSERT INTO dbo.security_mapping VALUES
    ('analyst@contoso.com', 'North America'),
    ('analyst@contoso.com', 'Europe'),
    ('manager@contoso.com', 'North America'),
    ('manager@contoso.com', 'Europe'),
    ('manager@contoso.com', 'Asia');

-- Step 2: Create the filter predicate function
CREATE FUNCTION dbo.fn_rls_region_filter(@region VARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS result
WHERE @region IN (
    SELECT sm.region
    FROM dbo.security_mapping sm
    WHERE sm.user_email = USER_NAME()
)
OR USER_NAME() = 'dbo';  -- Admin bypass

-- Step 3: Create the security policy
CREATE SECURITY POLICY dbo.RegionSecurityPolicy
    ADD FILTER PREDICATE dbo.fn_rls_region_filter(region)
    ON dbo.fact_sales
WITH (STATE = ON);

-- Step 4: Test as a specific user
EXECUTE AS USER = 'analyst@contoso.com';
SELECT COUNT(*) FROM dbo.fact_sales;  -- Only NA + Europe rows
REVERT;
```

## Quick Reference

| Component | Purpose | T-SQL |
|-----------|---------|-------|
| Filter predicate | Silently filter rows on SELECT | `ADD FILTER PREDICATE` |
| Block predicate | Prevent INSERT/UPDATE to unauthorized rows | `ADD BLOCK PREDICATE` |
| Security policy | Bind predicates to tables | `CREATE SECURITY POLICY` |
| `USER_NAME()` | Current user identity | Used in predicate functions |
| `SCHEMABINDING` | Required for predicate functions | `WITH SCHEMABINDING` |

## Common Mistakes

### Wrong

```sql
-- Granting UNMASK without considering RLS interaction
GRANT UNMASK TO [user@contoso.com];
-- This user can now see unmasked data but RLS still applies to rows
```

### Correct

```sql
-- Layer security: RLS for rows, masking for columns, CLS for column access
-- Always test the combined effect
EXECUTE AS USER = 'user@contoso.com';
SELECT * FROM dbo.fact_sales;  -- RLS filters rows, masking hides columns
REVERT;
```

## Security Layering

| Layer | Controls | Scope |
|-------|----------|-------|
| Workspace roles | Item access | Workspace-wide |
| Item permissions | Read/write/reshare | Per item |
| Row-level security | Row visibility | Per table |
| Column-level security | Column visibility | Per column |
| Dynamic data masking | Data obfuscation | Per column |

## Related

- [Dynamic Data Masking](../patterns/data-masking.md)
- [Warehouse Basics](../../04-data-warehouse/concepts/warehouse-basics.md)
- [T-SQL Patterns](../../04-data-warehouse/patterns/t-sql-patterns.md)
