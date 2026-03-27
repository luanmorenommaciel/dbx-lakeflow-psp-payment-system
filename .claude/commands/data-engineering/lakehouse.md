---
name: lakehouse
description: Table format and catalog guidance — delegates to lakehouse-architect agent
---

# Lakehouse Command

> Get guidance on Iceberg, Delta Lake, catalogs, and open table formats

## Usage

```bash
/lakehouse <description-or-question>
```

## Examples

```bash
/lakehouse "Set up Iceberg tables with partition evolution"
/lakehouse "Migrate from Hive to Iceberg on AWS"
/lakehouse "Compare Delta Lake vs Iceberg for our use case"
/lakehouse "Configure Unity Catalog for multi-cloud"
```

---

## What This Command Does

1. Invokes the **lakehouse-architect** agent
2. Analyzes your table format or catalog requirements
3. Loads KB patterns from `lakehouse` and `cloud-platforms` domains
4. Generates:
   - Table DDL with partition strategies
   - Catalog configuration (Unity, Polaris, Nessie)
   - Migration scripts from legacy formats
   - Compaction and maintenance procedures

## Agent Delegation

| Agent | Role |
|-------|------|
| `lakehouse-architect` | Primary — Iceberg, Delta, catalog governance |
| `data-platform-engineer` | Escalation — when infra provisioning is needed |
| `spark-engineer` | Escalation — when Spark read/write optimization is needed |

## KB Domains Used

- `lakehouse` — Iceberg v3, Delta Lake, catalog wars, DuckLake
- `cloud-platforms` — platform-specific Iceberg/Delta patterns
- `spark` — Spark table format integration

## Output

The agent generates table definitions, catalog configs, and operational runbooks for your lakehouse setup.
