# Generic Tests

> **Purpose**: Custom reusable test macros with parameters, severity, and tagging
> **MCP Validated**: 2026-03-26

## When to Use

- A test pattern repeats across many models (e.g., positive values, date ranges)
- Built-in tests (unique, not_null, accepted_values) don't cover your validation
- You want parameterized, reusable tests shared across your dbt project

## Implementation

```sql
-- macros/tests/test_positive_values.sql
-- Custom generic test: assert all values in a column are > 0

{% test positive_values(model, column_name, allow_zero=false) %}

    select {{ column_name }}
    from {{ model }}
    where {{ column_name }} {% if allow_zero %}< 0{% else %}<= 0{% endif %}
      and {{ column_name }} is not null

{% endtest %}
```

```sql
-- macros/tests/test_row_count_ratio.sql
-- Assert output row count is within expected ratio of input

{% test row_count_ratio(model, input_model, min_ratio=0.9, max_ratio=1.1) %}

    with output_count as (
        select count(*) as cnt from {{ model }}
    ),
    input_count as (
        select count(*) as cnt from {{ ref(input_model) }}
    )
    select
        o.cnt as output_rows,
        i.cnt as input_rows,
        o.cnt::float / nullif(i.cnt, 0) as ratio
    from output_count o
    cross join input_count i
    where o.cnt::float / nullif(i.cnt, 0) not between {{ min_ratio }} and {{ max_ratio }}

{% endtest %}
```

```yaml
# Apply custom tests in schema.yml
models:
  - name: fct_orders
    tests:
      - row_count_ratio:
          input_model: stg_stripe__orders
          min_ratio: 0.95
          max_ratio: 1.05
          severity: warn
    columns:
      - name: order_total
        tests:
          - positive_values:
              allow_zero: true
              severity: error
              tags: ['finance', 'critical']
              config:
                store_failures: true
                schema: test_results
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `severity` | `error` | `error` fails the pipeline, `warn` logs only |
| `store_failures` | `false` | Save failing rows to a table for debugging |
| `schema` | target schema | Schema for stored failure tables |
| `tags` | `[]` | Tags for selective test execution (`dbt test --select tag:finance`) |
| `enabled` | `true` | Disable test without removing it |

## Example Usage

```bash
# Run only critical tests
dbt test --select tag:critical

# Run tests for specific model
dbt test --select fct_orders

# Run tests and store failures
dbt test --store-failures
```

## See Also

- [testing-framework](../concepts/testing-framework.md)
- [macro-patterns](../patterns/macro-patterns.md)
