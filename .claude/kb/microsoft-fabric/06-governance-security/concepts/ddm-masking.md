> **MCP Validated:** 2026-02-17

# Dynamic Data Masking (DDM)

> **Purpose**: Deep reference for DDM functions, UNMASK permission management, and masking strategies in Fabric
> **Confidence**: 0.95

## Overview

Dynamic Data Masking (DDM) in Microsoft Fabric Warehouse obfuscates sensitive column data in query results without modifying the underlying data. Masking is enforced at query time based on user permissions. Fabric supports four masking functions: `default()`, `email()`, `random()`, and `partial()`. Users with the `UNMASK` permission see original values. DDM works alongside RLS and CLS for defense-in-depth security.

## Masking Functions

### default()

```sql
-- Masks the entire value based on data type
-- String -> "XXXX", Number -> 0, Date -> "1900-01-01", Binary -> 0x00
ALTER TABLE dbo.customers
ALTER COLUMN credit_card_number VARCHAR(20)
    MASKED WITH (FUNCTION = 'default()');

-- Masked output: XXXX
-- Unmasked output: 4111-1111-1111-1111
```

### email()

```sql
-- Shows first character, masks domain: aXXX@XXXX.com
ALTER TABLE dbo.customers
ALTER COLUMN email_address VARCHAR(200)
    MASKED WITH (FUNCTION = 'email()');

-- Masked output: jXXX@XXXX.com
-- Unmasked output: john.doe@contoso.com
```

### random(start, end)

```sql
-- Replaces numeric value with a random number in the given range
ALTER TABLE dbo.employees
ALTER COLUMN salary DECIMAL(12,2)
    MASKED WITH (FUNCTION = 'random(10000, 99999)');

-- Masked output: 47832.00 (random each query)
-- Unmasked output: 125000.00
```

### partial(prefix, padding, suffix)

```sql
-- Shows prefix characters, padding, then suffix characters
ALTER TABLE dbo.customers
ALTER COLUMN phone VARCHAR(20)
    MASKED WITH (FUNCTION = 'partial(3, "***-***-", 2)');

-- Masked output: 555***-***-67
-- Unmasked output: 555-123-4567

-- SSN example: show last 4 only
ALTER TABLE dbo.customers
ALTER COLUMN ssn VARCHAR(11)
    MASKED WITH (FUNCTION = 'partial(0, "XXX-XX-", 4)');

-- Masked output: XXX-XX-6789
-- Unmasked output: 123-45-6789
```

## UNMASK Permission

```sql
-- Grant full UNMASK on all masked columns
GRANT UNMASK TO [admin@contoso.com];

-- Grant column-level UNMASK (granular)
GRANT UNMASK ON dbo.customers(email_address) TO [support@contoso.com];

-- Grant table-level UNMASK
GRANT UNMASK ON dbo.customers TO [compliance_team];

-- Revoke UNMASK
REVOKE UNMASK FROM [former_admin@contoso.com];

-- Check who has UNMASK
SELECT
    dp.name AS principal_name,
    perm.permission_name,
    perm.state_desc,
    CASE
        WHEN perm.major_id = 0 THEN 'DATABASE'
        ELSE OBJECT_NAME(perm.major_id)
    END AS scope,
    CASE
        WHEN perm.minor_id > 0 THEN COL_NAME(perm.major_id, perm.minor_id)
        ELSE 'ALL COLUMNS'
    END AS column_scope
FROM sys.database_permissions perm
JOIN sys.database_principals dp ON perm.grantee_principal_id = dp.principal_id
WHERE perm.permission_name = 'UNMASK';
```

## Quick Reference

| Function | Syntax | Best For |
|----------|--------|----------|
| `default()` | `MASKED WITH (FUNCTION = 'default()')` | General PII |
| `email()` | `MASKED WITH (FUNCTION = 'email()')` | Email addresses |
| `random(s,e)` | `MASKED WITH (FUNCTION = 'random(1,100)')` | Numeric fields |
| `partial(p,pad,s)` | `MASKED WITH (FUNCTION = 'partial(3,"***",2)')` | Phone, SSN |

## Common Mistakes

### Wrong

```sql
-- Assuming DDM protects against data export or CTAS
SELECT * INTO dbo.copy_of_customers FROM dbo.customers;
-- If user has UNMASK, copied data is unmasked
-- If user lacks UNMASK, copied data contains masked values
```

### Correct

```sql
-- Combine DDM with CLS for sensitive columns
-- DDM obfuscates, CLS blocks entirely for highest protection
DENY SELECT ON dbo.customers(ssn) TO [intern@contoso.com];
-- Intern cannot even see masked SSN values
```

## Related

- [RLS Security](rls-security.md)
- [CLS Security](cls-security.md)
- [Dynamic Data Masking Pattern](../patterns/data-masking.md)
- [Compliance Audit](../patterns/compliance-audit.md)
