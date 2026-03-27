> **MCP Validated:** 2026-02-17

# Column-Level Security (CLS)

> **Purpose**: Implementing column-level security in Fabric Warehouse to restrict access to specific columns by role
> **Confidence**: 0.95

## Overview

Column-Level Security (CLS) in Microsoft Fabric Warehouse controls which users can see specific columns in a table. Unlike Dynamic Data Masking (which obfuscates values), CLS completely hides columns from unauthorized users -- queries referencing denied columns return an error. CLS uses standard T-SQL `GRANT` and `DENY` statements on individual columns and is enforced at the SQL engine level, including through the Lakehouse SQL analytics endpoint.

## The Pattern

```sql
-- Step 1: Create a table with sensitive columns
CREATE TABLE dbo.employee_data (
    employee_id     INT NOT NULL,
    full_name       VARCHAR(200) NOT NULL,
    department      VARCHAR(100) NOT NULL,
    email           VARCHAR(200) NOT NULL,
    salary          DECIMAL(12,2) NOT NULL,    -- Sensitive
    ssn             VARCHAR(11) NOT NULL,       -- Sensitive
    performance     VARCHAR(20) NOT NULL,       -- Sensitive
    hire_date       DATE NOT NULL
);

-- Step 2: Grant base SELECT on the table
GRANT SELECT ON dbo.employee_data TO [analyst@contoso.com];

-- Step 3: Deny access to sensitive columns
DENY SELECT ON dbo.employee_data(salary) TO [analyst@contoso.com];
DENY SELECT ON dbo.employee_data(ssn) TO [analyst@contoso.com];
DENY SELECT ON dbo.employee_data(performance) TO [analyst@contoso.com];

-- Step 4: Verify -- analyst can query non-sensitive columns
EXECUTE AS USER = 'analyst@contoso.com';

-- This works:
SELECT employee_id, full_name, department, email, hire_date
FROM dbo.employee_data;

-- This fails with permission error:
-- SELECT salary FROM dbo.employee_data;
-- Msg: The SELECT permission was denied on the column 'salary'

REVERT;
```

## Role-Based Column Access

```sql
-- Create database roles for different access levels
CREATE ROLE hr_role;
CREATE ROLE finance_role;
CREATE ROLE general_role;

-- General role: basic columns only
GRANT SELECT ON dbo.employee_data TO general_role;
DENY SELECT ON dbo.employee_data(salary) TO general_role;
DENY SELECT ON dbo.employee_data(ssn) TO general_role;
DENY SELECT ON dbo.employee_data(performance) TO general_role;

-- HR role: all except salary
GRANT SELECT ON dbo.employee_data TO hr_role;
DENY SELECT ON dbo.employee_data(salary) TO hr_role;

-- Finance role: all columns
GRANT SELECT ON dbo.employee_data TO finance_role;

-- Assign users to roles
ALTER ROLE general_role ADD MEMBER [analyst@contoso.com];
ALTER ROLE hr_role ADD MEMBER [hr_manager@contoso.com];
ALTER ROLE finance_role ADD MEMBER [cfo@contoso.com];
```

## Auditing Column Permissions

```sql
-- View all column-level permissions
SELECT
    dp.name AS principal_name,
    dp.type_desc AS principal_type,
    perm.permission_name,
    perm.state_desc AS grant_or_deny,
    OBJECT_NAME(perm.major_id) AS table_name,
    COL_NAME(perm.major_id, perm.minor_id) AS column_name
FROM sys.database_permissions perm
JOIN sys.database_principals dp
    ON perm.grantee_principal_id = dp.principal_id
WHERE perm.minor_id > 0  -- Column-level only
ORDER BY dp.name, table_name, column_name;
```

## Quick Reference

| Operation | T-SQL | Effect |
|-----------|-------|--------|
| Hide column | `DENY SELECT ON table(col) TO user` | Query on column returns error |
| Reveal column | `GRANT SELECT ON table(col) TO user` | Column accessible |
| Remove restriction | `REVOKE SELECT ON table(col) TO user` | Inherits parent permission |
| Check permissions | `HAS_PERMS_BY_NAME('table', 'OBJECT', 'SELECT')` | Returns 1 or 0 |

## CLS vs Dynamic Data Masking

| Feature | CLS | Dynamic Data Masking |
|---------|-----|---------------------|
| Column visibility | Hidden entirely | Visible but obfuscated |
| Error on access | Yes (permission denied) | No (returns masked value) |
| SELECT * behavior | Error if any denied column | Returns masked values |
| Performance impact | Minimal | Minimal |
| Granularity | Column per principal | Column per principal |

## Common Mistakes

### Wrong

```sql
-- Using SELECT * with CLS -- will fail if any column is denied
SELECT * FROM dbo.employee_data;
```

### Correct

```sql
-- Always specify columns explicitly when CLS is in effect
SELECT employee_id, full_name, department, email, hire_date
FROM dbo.employee_data;
```

## Related

- [RLS Security](rls-security.md)
- [DDM Masking](ddm-masking.md)
- [Dynamic Data Masking](../patterns/data-masking.md)
- [Compliance Audit](../patterns/compliance-audit.md)
