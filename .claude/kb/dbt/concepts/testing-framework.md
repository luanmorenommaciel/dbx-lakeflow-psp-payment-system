# Testing Framework

> **Purpose**: dbt test types — generic, singular, custom macros, unit tests (v1.8+), severity, store_failures
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26 | Updated with unit testing (v1.8+)

## Overview

dbt testing ensures data quality at the model level. **Generic tests** (unique, not_null, accepted_values, relationships) are declared in YAML. **Singular tests** are standalone SQL files returning failing rows. **Custom generic tests** are Jinja macros that accept parameters. **Unit tests** (v1.8+) validate SQL modeling logic on static inputs before materialization, enabling test-driven development. Tests run via `dbt test` or `dbt build`.

## The Concept

```yaml
# models/staging/stripe/_stripe__models.yml
version: 2

models:
  - name: stg_stripe__payments
    description: "Staging model for Stripe payments"
    columns:
      - name: payment_id
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: payment_status
        tests:
          - accepted_values:
              values: ['succeeded', 'pending', 'failed', 'refunded']
              severity: warn
      - name: order_id
        tests:
          - relationships:
              to: ref('stg_orders')
              field: order_id
```

## Unit Tests (v1.8+)

```yaml
# models/marts/finance/_finance__unit_tests.yml
unit_tests:
  - name: test_order_total_calculation
    description: "Verify order_total = quantity * unit_price"
    model: fct_orders
    given:
      - input: ref('stg_stripe__orders')
        rows:
          - {order_id: 1, quantity: 3, unit_price: 10.00, discount_amount: 5.00}
          - {order_id: 2, quantity: 1, unit_price: 25.00, discount_amount: 0}
    expect:
      rows:
        - {order_id: 1, order_total: 30.00, net_revenue: 25.00}
        - {order_id: 2, order_total: 25.00, net_revenue: 25.00}
```

## Quick Reference

| Test Type | Location | Returns | Use When | Min Version |
|-----------|----------|---------|----------|-------------|
| Generic (built-in) | schema.yml | Failing rows | Standard column checks | All |
| Singular | tests/*.sql | Failing rows | Complex multi-table logic | All |
| Custom generic | macros/tests/ | Failing rows | Reusable parameterized checks | All |
| **Unit test** | models/*.yml | Pass/fail | Validate SQL logic on static inputs | **v1.8+** |
| Source freshness | sources.yml | Stale flag | Monitoring upstream data | All |

### Unit Test Limitations

- SQL models only (not Python models)
- Models in current project only
- No `materialized view` models
- No recursive SQL or introspective queries
- Table names must be aliased to unit test `JOIN` logic

## Common Mistakes

### Wrong

```yaml
# No tests on a model — data quality unknown
models:
  - name: fct_orders
    columns:
      - name: order_id
        # No tests! Duplicates will silently corrupt downstream
```

### Correct

```yaml
models:
  - name: fct_orders
    tests:
      - dbt_utils.recency:
          datepart: hour
          field: updated_at
          interval: 24
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
      - name: order_total
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: true
```

## Related

- [generic-tests](../patterns/generic-tests.md)
- [model-types](../concepts/model-types.md)
