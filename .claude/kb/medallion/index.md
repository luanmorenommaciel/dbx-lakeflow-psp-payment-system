# Medallion Architecture Knowledge Base

> **Purpose**: Bronze/Silver/Gold layered data architecture for lakehouses, data quality, schema evolution, incremental loading, AI-era extensions
> **MCP Validated**: 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/bronze-layer.md](concepts/bronze-layer.md) | Raw ingestion layer -- append-only, schema-on-read |
| [concepts/silver-layer.md](concepts/silver-layer.md) | Cleansed/conformed layer -- deduplicated, typed, validated |
| [concepts/gold-layer.md](concepts/gold-layer.md) | Business aggregation layer -- star schemas, KPIs, SCD |
| [concepts/domain-modeling.md](concepts/domain-modeling.md) | Domain-driven design applied to data mesh and lakehouse |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/layer-transitions.md](patterns/layer-transitions.md) | Bronze to Silver to Gold transformation flow |
| [patterns/data-quality-gates.md](patterns/data-quality-gates.md) | Quality checks and quarantine between layers |
| [patterns/schema-evolution.md](patterns/schema-evolution.md) | Schema evolution strategy with Delta Lake |
| [patterns/incremental-loading.md](patterns/incremental-loading.md) | Incremental/merge patterns with MERGE INTO |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/medallion-config.yaml](specs/medallion-config.yaml) | Architecture configuration specification |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Bronze Layer** | Raw, append-only ingestion preserving source fidelity with metadata columns |
| **Silver Layer** | Cleansed, deduplicated, conformed data with enforced schemas and SCD Type 1/2 |
| **Gold Layer** | Business-level aggregates, star schemas, and pre-computed KPIs for consumption |
| **Domain Modeling** | Organizing lakehouse tables by business domains rather than technical layers |
| **Quality Gates** | Automated data quality checks that quarantine bad records between layers |
| **AI Extensions** | Feature layer and vector layer extending medallion for ML/AI workloads (2025+) |
| **Contracts as Code** | Medallion is a set of contracts, not just a pipeline — enforce at every boundary |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/bronze-layer.md, concepts/silver-layer.md, concepts/gold-layer.md |
| **Intermediate** | patterns/layer-transitions.md, patterns/data-quality-gates.md |
| **Advanced** | patterns/schema-evolution.md, patterns/incremental-loading.md, concepts/domain-modeling.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| medallion-architect | All concepts + layer-transitions | Designing lakehouse layer architecture |
| data-quality-engineer | data-quality-gates, silver-layer | Implementing quality checks and validation |
| lakehouse-engineer | incremental-loading, schema-evolution | Building production ETL pipelines |
