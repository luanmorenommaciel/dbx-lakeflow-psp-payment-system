---
name: schema
description: Interactive schema design — delegates to schema-designer agent
---

# Schema Command

> Design data models interactively (star schema, Data Vault, SCD, normalization)

## Usage

```bash
/schema <description-or-file>
```

## Examples

```bash
/schema "Star schema for e-commerce analytics"
/schema "SCD Type 2 for customer dimension"
/schema "Data Vault for multi-source integration"
/schema requirements/data-model-spec.md
```

---

## What This Command Does

1. Invokes the **schema-designer** agent
2. Analyzes your modeling requirements
3. Loads KB patterns from `data-modeling` and `sql-patterns` domains
4. Generates:
   - Entity-relationship diagrams (ASCII)
   - DDL statements for target platform
   - Grain definition and cardinality analysis
   - SCD strategy recommendations

## Agent Delegation

| Agent | Role |
|-------|------|
| `schema-designer` | Primary — dimensional modeling, SCD, Data Vault |
| `dbt-specialist` | Escalation — when models need dbt implementation |
| `sql-optimizer` | Escalation — when index strategy is needed |

## KB Domains Used

- `data-modeling` — dimensional modeling, SCD types, normalization, schema evolution
- `sql-patterns` — DDL patterns, index strategies
- `data-quality` — constraint enforcement

## Output

The agent generates DDL, model documentation, and implementation recommendations for your target platform.
