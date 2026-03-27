# Data Modeling Knowledge Base

> **Purpose**: Schema design — dimensional modeling, Data Vault 2.0, SCD types, OBT debate, schema evolution (Iceberg/Delta/Avro)
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/dimensional-modeling.md](concepts/dimensional-modeling.md) | Kimball facts, dimensions, bus matrix |
| [concepts/scd-types.md](concepts/scd-types.md) | SCD Types 1-6 with SQL patterns |
| [concepts/normalization.md](concepts/normalization.md) | 1NF-BCNF, when to denormalize |
| [concepts/schema-evolution.md](concepts/schema-evolution.md) | Iceberg/Avro evolution, compatibility |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/star-schema.md](patterns/star-schema.md) | Fact/dimension DDL, surrogate keys |
| [patterns/data-vault.md](patterns/data-vault.md) | Hubs, links, satellites, hash keys |
| [patterns/one-big-table.md](patterns/one-big-table.md) | OBT pattern for simple analytics |
| [patterns/schema-migration.md](patterns/schema-migration.md) | Backward-compatible migrations |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Dimensional Modeling** | Kimball star schema — facts (events) + dimensions (attributes) |
| **SCD Types** | Track historical changes: Type 1 (overwrite) through Type 6 (hybrid) |
| **Schema Evolution** | Add/rename/drop columns without breaking consumers (Iceberg v3, Delta 4.x, Avro) |
| **Data Vault 2.0** | Hub-link-satellite model for enterprise-scale warehousing; automation via AutomateDV/dbt |
| **One Big Table (OBT)** | Denormalized wide table for BI dashboards; trade-offs vs star schema |
| **Modeling in 2025+** | Star schema + semantic layer, OBT for dashboards, Data Vault for enterprise integration |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/dimensional-modeling.md |
| **Intermediate** | patterns/star-schema.md |
| **Advanced** | patterns/data-vault.md, concepts/schema-evolution.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| schema-designer | All files | Schema creation, modeling decisions |
| define-agent | concepts/dimensional-modeling.md | Requirements extraction |
