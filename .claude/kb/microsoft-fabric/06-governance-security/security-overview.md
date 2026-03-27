> **MCP Validated:** 2026-02-17

# Security Overview: Microsoft Fabric Governance & Security

> **Purpose**: Comprehensive security reference for Fabric Warehouse, Lakehouse, and workspace-level protection
> **Confidence**: 0.95

## Overview

Microsoft Fabric provides a layered security model spanning workspace access, item-level permissions, and data-level controls. The three core data-level security mechanisms are: **Row-Level Security (RLS)** for filtering rows by user identity, **Column-Level Security (CLS)** for restricting column visibility by role, and **Dynamic Data Masking (DDM)** for obfuscating sensitive values without denying access. These work alongside workspace roles, service principal authentication, sensitivity labels, and network isolation.

## Security Layers

```text
┌─────────────────────────────────────────────────┐
│  Layer 1: Workspace Access                       │
│  - Roles: Admin, Member, Contributor, Viewer     │
│  - Service Principal authentication              │
│  - Azure AD / Entra ID integration               │
├─────────────────────────────────────────────────┤
│  Layer 2: Item-Level Permissions                 │
│  - Per-item sharing (datasets, reports)          │
│  - Build permissions for semantic models         │
│  - SQL object-level GRANT/DENY                   │
├─────────────────────────────────────────────────┤
│  Layer 3: Data-Level Security                    │
│  - RLS: Row-Level Security (filter predicates)   │
│  - CLS: Column-Level Security (GRANT/DENY cols)  │
│  - DDM: Dynamic Data Masking (obfuscation)       │
├─────────────────────────────────────────────────┤
│  Layer 4: Data Protection                        │
│  - Sensitivity labels (Microsoft Purview)        │
│  - Encryption at rest and in transit             │
│  - Managed VNet / Private endpoints              │
└─────────────────────────────────────────────────┘
```

## Row-Level Security (RLS)

Restricts which rows a user can see based on predicate functions.

```sql
-- Create filter predicate
CREATE FUNCTION dbo.fn_rls_filter(@region VARCHAR(50))
RETURNS TABLE WITH SCHEMABINDING AS
RETURN SELECT 1 AS result
WHERE @region IN (
    SELECT region FROM dbo.security_mapping
    WHERE user_email = USER_NAME()
) OR USER_NAME() = 'dbo';

-- Apply security policy
CREATE SECURITY POLICY dbo.RegionPolicy
    ADD FILTER PREDICATE dbo.fn_rls_filter(region)
    ON dbo.fact_sales WITH (STATE = ON);
```

**Key constraints:** Filter predicates only (no block predicates in Fabric). Works in Warehouse (full) and Lakehouse SQL endpoint (read-only).

## Column-Level Security (CLS)

Hides entire columns from unauthorized users via GRANT/DENY.

```sql
GRANT SELECT ON dbo.employees TO [analyst@contoso.com];
DENY SELECT ON dbo.employees(salary) TO [analyst@contoso.com];
DENY SELECT ON dbo.employees(ssn) TO [analyst@contoso.com];
```

**Key constraints:** Queries referencing denied columns return errors (not masked values). Use SELECT with explicit column lists.

## Dynamic Data Masking (DDM)

Obfuscates sensitive data while allowing queries to succeed.

```sql
ALTER TABLE dbo.customers
ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');
ALTER TABLE dbo.customers
ALTER COLUMN phone ADD MASKED WITH (FUNCTION = 'partial(0,"XXX-XXX-",4)');
ALTER TABLE dbo.customers
ALTER COLUMN credit_card ADD MASKED WITH (FUNCTION = 'default()');
```

**Masking functions:** `default()`, `email()`, `random(start, end)`, `partial(prefix, padding, suffix)`.

## Service Principal Authentication

```text
Workspace Settings → Manage access → Add member
→ Search for App Registration name
→ Assign role: Contributor (for pipelines) or Member (for admin)
```

Required for CI/CD pipelines and automated deployments. Service principals need explicit workspace role assignment.

## When to Use Each Mechanism

| Need | Mechanism | Scope |
|------|-----------|-------|
| Users see only their data rows | RLS | Row filtering |
| Hide sensitive columns entirely | CLS | Column access |
| Show partial/masked values | DDM | Value obfuscation |
| Control workspace access | Workspace roles | Item visibility |
| Label and track sensitive data | Sensitivity labels | Data classification |
| Automate securely | Service principals | API/pipeline auth |

## Related KB Files

- [RLS Implementation](concepts/rls-security.md)
- [CLS Implementation](concepts/cls-security.md)
- [DDM Masking](concepts/ddm-masking.md)
- [Purview Integration](concepts/purview-integration.md)
- [Data Masking Patterns](patterns/data-masking.md)
- [Compliance Audit](patterns/compliance-audit.md)
- [Sensitivity Labels](patterns/sensitivity-labels.md)
