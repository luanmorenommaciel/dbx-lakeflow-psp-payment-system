# Data Contracts

> **Purpose**: ODCS format, Soda Data Contracts, dbt contracts — enforcement and ownership
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Data contracts are formal agreements between data producers and consumers that define schema, quality SLAs, and ownership. The **Open Data Contract Standard (ODCS)** provides a vendor-neutral YAML format -- now at **v3.1.0** (Dec 2025) with relationship support and stricter validation. dbt contracts enforce column-level types at build time. Soda 4.0 makes contracts the default paradigm. Contracts shift quality left -- catching issues before downstream impact.

**Key 2025-2026 updates:**
- **ODCS v3.1.0** (Dec 2025) -- relationships (FK), stricter JSON Schema validation, richer metadata, backward-compatible with v3.0
- **Soda 4.0** (Jan 2026) -- Data Contracts as default syntax, AI-translated rules, multi-source support
- **datacontract-cli** -- Soda integration for check generation, ODCS import, SQL DDL export, HTML catalog
- **Linux Foundation AI & Data** -- ODCS is now an incubation-level project under LF AI & Data

## The Concept

```yaml
# Open Data Contract Standard (ODCS) v3.1
apiVersion: v3.1.0
kind: DataContract
metadata:
  name: orders
  version: 1.2.0
  owner: data-engineering@company.com
  domain: commerce

schema:
  type: object
  properties:
    order_id:
      type: string
      format: uuid
      required: true
      unique: true
    customer_id:
      type: string
      required: true
    amount:
      type: number
      minimum: 0
    order_date:
      type: string
      format: date
    status:
      type: string
      enum: [pending, completed, cancelled, refunded]

# NEW in v3.1: Relationships (FK references)
references:
  - name: customer_ref
    column: customer_id
    referencedDataset: customers
    referencedColumn: customer_id
    type: foreignKey

quality:
  freshness:
    maxStaleness: PT1H  # ISO 8601: 1 hour
  completeness:
    threshold: 0.99
  volume:
    minRows: 1000
    maxGrowthRate: 3.0  # alert if 3x normal volume

sla:
  availability: 99.9%
  latency: PT30M
```

## Quick Reference

| Framework | Contract Type | Enforcement Point | Format | Latest |
|-----------|--------------|-------------------|--------|--------|
| ODCS | Schema + SLA + relationships | CI/CD pipeline | YAML | v3.1.0 (Dec 2025) |
| dbt contracts | Column types | `dbt build` | YAML (schema.yml) | dbt Core 1.9+ |
| Soda | Data Contracts (v4) / Runtime checks (v3) | Contract verification | YAML | v4.0 (Jan 2026) |
| Protobuf/Avro | Wire schema | Serialization | `.proto`/`.avsc` | -- |
| datacontract-cli | ODCS + Soda + exports | CLI validation | YAML | Active |

## Common Mistakes

### Wrong

```yaml
# Contract with no ownership — who do you call when it breaks?
schema:
  columns:
    - name: order_id
      type: string
# Missing: owner, version, SLA, quality thresholds
```

### Correct

```yaml
# Contract with full ownership and SLA
metadata:
  owner: data-engineering@company.com
  version: 1.2.0
  slack_channel: "#data-orders"
quality:
  freshness:
    maxStaleness: PT1H
sla:
  availability: 99.9%
```

## Related

- [data-contract-authoring](../patterns/data-contract-authoring.md)
- [quality-dimensions](../concepts/quality-dimensions.md)
