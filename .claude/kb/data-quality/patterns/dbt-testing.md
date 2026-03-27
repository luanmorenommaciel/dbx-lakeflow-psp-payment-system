# dbt Testing

> **Purpose**: schema.yml tests, singular tests, custom generic tests, source freshness, store_failures
> **MCP Validated**: 2026-03-26

## When to Use

- Validating data quality as part of the dbt build pipeline
- Enforcing primary key uniqueness and not-null constraints
- Checking referential integrity between models
- Monitoring source freshness before running transformations

## Implementation

```yaml
# models/marts/finance/_finance__models.yml
version: 2

models:
  - name: fct_orders
    description: "Order fact table — one row per order"
    config:
      contract:
        enforced: true  # dbt contract — enforces column types at build
    columns:
      - name: order_id
        data_type: varchar
        description: "Primary key"
        tests:
          - unique
          - not_null
      - name: customer_id
        data_type: varchar
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id
      - name: amount
        data_type: numeric(12,2)
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              inclusive: true
      - name: status
        data_type: varchar
        tests:
          - accepted_values:
              values: ['pending', 'completed', 'cancelled', 'refunded']
              config:
                severity: error
```

```sql
-- tests/singular/assert_no_orphan_orders.sql
-- Singular test: custom SQL that returns failing rows
SELECT o.order_id
FROM {{ ref('fct_orders') }} o
LEFT JOIN {{ ref('dim_customers') }} c
    ON o.customer_id = c.customer_id
WHERE c.customer_id IS NULL
```

```sql
-- macros/tests/test_row_count_above.sql
-- Custom generic test: reusable across models
{% test row_count_above(model, min_count) %}
SELECT 1
FROM (
    SELECT COUNT(*) AS row_count FROM {{ model }}
) sub
WHERE row_count < {{ min_count }}
{% endtest %}
```

```yaml
# models/staging/_sources.yml
sources:
  - name: stripe
    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}
    loaded_at_field: _loaded_at
    tables:
      - name: payments
      - name: customers
```

## Configuration

| Feature | Config | Description |
|---------|--------|-------------|
| `store_failures` | `config: {store_failures: true}` | Save failing rows to `_dbt_test_audit` |
| `severity` | `config: {severity: warn}` | `warn` or `error` — warn doesn't fail the build |
| `where` | `config: {where: "status != 'draft'"}` | Filter test scope |
| `limit` | `config: {limit: 100}` | Cap failing rows stored |
| `contract` | `config: {contract: {enforced: true}}` | Column-level type enforcement |

## Example Usage

```bash
# Run all tests
dbt test

# Run tests for specific model + upstream
dbt test --select +fct_orders

# Check source freshness
dbt source freshness

# Run tests and store failures
dbt test --store-failures
```

## See Also

- [great-expectations](../patterns/great-expectations.md)
- [schema-validation](../patterns/schema-validation.md)
