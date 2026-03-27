# Data Quality Knowledge Base

> **Purpose**: Data quality, contracts, and observability — Soda, GE, dbt tests, ODCS, Monte Carlo
> **MCP Validated**: 2026-03-26
> **Latest**: Soda 4.0 (Jan 2026) with Data Contracts as default, GX Core 1.3+ with ExpectAI and Atlan integration, ODCS v3.1.0 with relationships and stricter validation

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/data-contracts.md](concepts/data-contracts.md) | ODCS, Soda, dbt contracts, ownership |
| [concepts/quality-dimensions.md](concepts/quality-dimensions.md) | Completeness, accuracy, consistency, timeliness, uniqueness, validity |
| [concepts/observability.md](concepts/observability.md) | Freshness, volume, distribution drift |
| [concepts/soda-core.md](concepts/soda-core.md) | Soda 4.0 Data Contracts, SodaCL syntax, anomaly detection |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/great-expectations.md](patterns/great-expectations.md) | Suites, checkpoints, data docs |
| [patterns/dbt-testing.md](patterns/dbt-testing.md) | schema.yml tests, singular, freshness |
| [patterns/schema-validation.md](patterns/schema-validation.md) | JSON Schema, Pydantic, Avro enforcement |
| [patterns/data-contract-authoring.md](patterns/data-contract-authoring.md) | ODCS YAML, SLAs, CI/CD enforcement |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| data-quality-analyst | All files | Quality rules, contracts, monitoring |
| code-reviewer | patterns/dbt-testing.md | Data engineering review |
