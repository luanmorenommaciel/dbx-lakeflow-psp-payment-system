# Data Contract Authoring

> **Purpose**: Full ODCS v3.1 YAML template with schema, relationships, SLA, ownership, CI/CD enforcement
> **MCP Validated**: 2026-03-26

## When to Use

- Defining a formal interface between data producers and consumers
- Establishing SLAs for freshness, completeness, and availability
- Enabling CI/CD validation of schema changes (breaking change detection)
- Implementing data mesh ownership boundaries

## Implementation

```yaml
# contracts/commerce/orders.contract.yaml
# Open Data Contract Standard (ODCS) v3.1
apiVersion: v3.1.0
kind: DataContract
metadata:
  name: orders
  version: 2.1.0
  description: "Commerce order events — one record per order state change"
  owner: commerce-data-team@company.com
  domain: commerce
  tags: [finance, revenue, critical]
  slack_channel: "#data-commerce-alerts"
  classification: internal

schema:
  type: table
  database: analytics
  schema: silver
  table: orders
  columns:
    - name: order_id
      type: varchar(36)
      required: true
      unique: true
      description: "UUID primary key"
    - name: customer_id
      type: varchar(36)
      required: true
      description: "FK to dim_customers"
    - name: amount
      type: decimal(12,2)
      required: true
      constraints:
        minimum: 0
    - name: currency_code
      type: varchar(3)
      required: true
      constraints:
        pattern: "^[A-Z]{3}$"
    - name: status
      type: varchar(20)
      required: true
      constraints:
        enum: [pending, completed, cancelled, refunded]
    - name: order_date
      type: date
      required: true
    - name: updated_at
      type: timestamp_tz
      required: true

# NEW in ODCS v3.1: Relationships
references:
  - name: customer_fk
    column: customer_id
    referencedDataset: dim_customers
    referencedColumn: customer_id
    type: foreignKey
    description: "Links orders to customer dimension"

quality:
  freshness:
    column: updated_at
    maxStaleness: PT1H        # ISO 8601: 1 hour max
    schedule: "*/15 * * * *"  # check every 15 minutes
  completeness:
    order_id: 1.0             # 100% non-null
    customer_id: 1.0
    amount: 0.999             # 99.9% non-null
  volume:
    minRowsPerDay: 500
    maxGrowthRate: 5.0        # alert if 5x normal daily volume
  uniqueness:
    order_id: 1.0             # 100% unique

sla:
  availability: 99.9%
  latency: PT30M              # data available within 30 min of event
  support_hours: "09:00-18:00 UTC"
  escalation:
    p1: pagerduty://commerce-oncall
    p2: slack://#data-commerce-alerts

evolution:
  compatibility: backward      # new versions must be backward compatible
  deprecation_policy: "90 days notice via #data-announcements"
  breaking_changes:
    - "Removing a required column"
    - "Changing column type to incompatible type"
    - "Renaming a column"

consumers:
  - name: finance-reporting
    owner: finance-analytics@company.com
    usage: "Daily revenue aggregation"
  - name: ml-churn-model
    owner: ml-team@company.com
    usage: "Feature extraction for churn prediction"
```

## Configuration (ODCS v3.1 Sections)

| Section | Required | Purpose |
|---------|----------|---------|
| `metadata` | Yes | Ownership, versioning, classification |
| `schema` | Yes | Column definitions, types, constraints |
| `references` | Recommended | FK relationships to other datasets (NEW in v3.1) |
| `quality` | Recommended | Freshness, completeness, volume thresholds |
| `sla` | Recommended | Availability, latency, escalation |
| `evolution` | Recommended | Compatibility rules, deprecation policy |
| `consumers` | Optional | Registered downstream dependents |
| `vendors` | Optional | Vendor-specific extensions |
| `team` | Optional | Team members and contact info |
| `roles` | Optional | Access control roles |
| `pricing` | Optional | Data product pricing (data mesh) |
| `infrastructures` | Optional | Server and infrastructure details |

## Example Usage

```bash
# CI/CD: validate contract on PR
datacontract lint contracts/commerce/orders.contract.yaml

# Check for breaking changes between versions
datacontract breaking contracts/commerce/orders.contract.yaml \
  --against main:contracts/commerce/orders.contract.yaml

# Generate dbt schema.yml from contract
datacontract export --format dbt contracts/commerce/orders.contract.yaml
```

## See Also

- [data-contracts](../concepts/data-contracts.md)
- [schema-validation](../patterns/schema-validation.md)
