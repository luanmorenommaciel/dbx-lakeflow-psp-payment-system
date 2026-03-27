# Mesh Architecture

> **Purpose**: dbt Mesh multi-project setup with cross-project refs, access modifiers, Semantic Layer
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26 | Updated with production migration patterns

## Overview

dbt Mesh enables domain teams to own independent dbt projects with **cross-project references**, **access modifiers** (public/protected/private), and **model versioning**. Combined with the **Semantic Layer** (MetricFlow), it implements Data Mesh principles: federated ownership with centralized metric definitions. Each project publishes its public models as a data product. Cross-project refs require dbt Cloud Enterprise or Enterprise+ plans. Catalog provides full cross-project lineage.

## The Concept

```yaml
# models/marts/orders/_orders__models.yml
# Publishing a model as a public API for cross-project consumption

version: 2

models:
  - name: fct_orders
    access: public          # Visible to other dbt projects
    group: finance_team     # Ownership boundary
    latest_version: 2       # Consumers default to this version

    versions:
      - v: 2
        columns:
          - include: all
          - name: order_total_usd
            description: "Order total in USD (added in v2)"
      - v: 1
        defined_in: fct_orders_v1  # Old version still available

    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

## Quick Reference

| Access Level | Visibility | Use When |
|-------------|-----------|----------|
| `public` | Any project can ref() | Stable API, data product |
| `protected` | Same project + same group | Shared within team |
| `private` | Same project only | Internal implementation detail |

| Mesh Concept | dbt Feature |
|-------------|------------|
| Domain ownership | `group:` in model config |
| Data product API | `access: public` models |
| Contract | `contract: {enforced: true}` |
| Versioning | `versions:` with `latest_version` |
| Centralized metrics | Semantic Layer (MetricFlow) |

## Migration Heuristics (Production Mesh Adoption)

| Step | Question | Action |
|------|----------|--------|
| 1 | Is there a single downstream team? | Start there as a consumer |
| 2 | Are people working on separate transformation levels? | Split by staging vs marts |
| 3 | Different data sources handled separately? | Split by domain boundary |
| 4 | Existing logical groupings in your project? | Formalize as Mesh interfaces |

## Common Mistakes

### Wrong

```yaml
# Exposing internal models as public without contracts
models:
  - name: int_orders_pivoted
    access: public  # Internal model exposed — no contract, will break consumers
```

### Correct

```yaml
# Only publish stable mart models with enforced contracts
models:
  - name: fct_orders
    access: public
    config:
      contract:
        enforced: true  # Schema changes must be backward-compatible
    columns:
      - name: order_id
        data_type: string  # Enforced type contract
        tests: [unique, not_null]
```

## Related

- [fusion-engine](../concepts/fusion-engine.md)
- [semantic-layer](../patterns/semantic-layer.md)
