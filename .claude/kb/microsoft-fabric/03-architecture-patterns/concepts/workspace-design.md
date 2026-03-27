> **MCP Validated:** 2026-02-17

# Workspace Design

> **Purpose**: Workspace organization strategies, roles, identity, and Git integration
> **Confidence**: 0.95

## Overview

Workspaces are the primary organizational unit in Microsoft Fabric. They serve as containers for items (Lakehouses, Warehouses, Notebooks, Pipelines, Semantic Models) and define the security boundary for access control. A well-designed workspace strategy balances team autonomy, governance, and operational efficiency. Every workspace maps to a OneLake folder, so workspace design directly affects storage layout.

## Organization Strategies

### Domain-Based Workspaces (Recommended)

```text
FABRIC TENANT
  |
  +-- ws-finance-dev        --> Finance team development
  +-- ws-finance-prod       --> Finance production items
  +-- ws-marketing-dev      --> Marketing team development
  +-- ws-marketing-prod     --> Marketing production items
  +-- ws-shared-gold-prod   --> Cross-domain gold layer
  +-- ws-platform-infra     --> Shared pipelines, admin notebooks
```

### Layer-Based Workspaces

```text
FABRIC TENANT
  |
  +-- ws-bronze-prod        --> All raw ingestion
  +-- ws-silver-prod        --> All cleansed/conformed
  +-- ws-gold-prod          --> All business-ready
  +-- ws-dev                --> Development sandbox
```

### Strategy Comparison

| Strategy | Pros | Cons | Best For |
|----------|------|------|----------|
| Domain-based | Team autonomy, clear ownership | More workspaces to manage | Large orgs, multiple teams |
| Layer-based | Simple structure, clear data flow | Cross-team contention | Small orgs, single team |
| Hybrid (domain + layer) | Balanced governance | Complex naming conventions | Mid-size organizations |

## Workspace Roles

| Role | Permissions | Typical Assignment |
|------|-------------|--------------------|
| **Admin** | Full control, manage access, delete workspace | Platform team leads |
| **Member** | Create/edit/delete items, share items | Data engineers |
| **Contributor** | Create/edit items, cannot share or manage access | Developers |
| **Viewer** | Read-only access to all items | Business analysts, stakeholders |

### Role Assignment Best Practices

- Use Entra ID (Azure AD) security groups, not individual users
- Assign the minimum role needed (principle of least privilege)
- Use separate groups per workspace per role
- Naming convention: `sg-fabric-{domain}-{env}-{role}`

```text
Example groups:
  sg-fabric-finance-prod-admin        --> 2-3 team leads
  sg-fabric-finance-prod-member       --> 5-8 data engineers
  sg-fabric-finance-prod-contributor  --> 3-5 developers
  sg-fabric-finance-prod-viewer       --> 20+ analysts
```

## Workspace Identity

- Each workspace can have a **managed identity** for accessing external resources
- Use workspace identity instead of personal credentials for pipelines
- Grants the workspace itself (not individual users) access to sources
- Supported for Azure SQL, ADLS Gen2, and other Azure services

```text
Pipeline --> Workspace Identity --> Azure SQL Database
  (no personal credentials stored in pipeline)
```

## Git Integration

| Setting | Recommendation | Notes |
|---------|----------------|-------|
| Repository | Azure DevOps or GitHub | One repo per domain |
| Branch | `main` for prod workspace | `dev` for dev workspace |
| Sync direction | Git --> Workspace (CI/CD) | Avoid manual edits in prod |
| Supported items | Notebooks, Pipelines, Semantic Models, Reports | Not all item types supported |
| Conflict resolution | Git wins (for prod) | Manual merge for dev |

### Git Workflow

```text
Developer Workspace (dev branch)
  |
  +-- Edit notebook / pipeline
  +-- Commit to dev branch
  +-- Pull Request --> main branch
  |
Production Workspace (main branch)
  +-- Auto-sync from main
  +-- Items updated automatically
```

## Naming Conventions

| Component | Pattern | Example |
|-----------|---------|---------|
| Workspace | `ws-{domain}-{env}` | `ws-finance-prod` |
| Lakehouse | `lh_{domain}_{layer}` | `lh_finance_bronze` |
| Warehouse | `wh_{domain}` | `wh_finance` |
| Pipeline | `pl_{domain}_{action}` | `pl_finance_daily_load` |
| Notebook | `nb_{domain}_{purpose}` | `nb_finance_silver_transform` |
| Semantic Model | `sm_{domain}_{subject}` | `sm_finance_revenue` |

## Common Mistakes

### Wrong

```text
Single workspace for everything: dev items, prod data, all teams
--> No access control, accidental deletions, no CI/CD possible
```

### Correct

```text
Separate workspaces per domain and environment, Git-connected,
with role-based access using security groups
```

## Related

- [Capacity Planning](capacity-planning.md)
- [Workload Selection](workload-selection.md)
- [Medallion in Fabric](../patterns/medallion-fabric.md)
