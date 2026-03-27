# Snapshot SCD Type 2

> **Purpose**: Track historical changes in mutable source data using dbt snapshots
> **MCP Validated**: 2026-03-26

## When to Use

- Source rows are updated in place (e.g., customer address changes)
- You need to answer "what was the value at point in time X?"
- Building slowly changing dimensions (Type 2) for a dimensional model
- Audit trail requirements for compliance

## Implementation

```sql
-- snapshots/snap_customers.sql
-- SCD Type 2 using timestamp strategy

{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    full_name,
    email,
    address_line_1,
    city,
    state,
    country,
    tier,                -- loyalty tier: bronze, silver, gold, platinum
    is_active,
    updated_at
from {{ source('crm', 'customers') }}

{% endsnapshot %}
```

```sql
-- Alternative: check_cols strategy (when no reliable updated_at exists)
{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='check',
        check_cols=['price', 'category', 'is_active']
    )
}}

select
    product_id,
    product_name,
    price,
    category,
    is_active
from {{ source('catalog', 'products') }}

{% endsnapshot %}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `strategy` | — | `timestamp` (use updated_at) or `check` (compare columns) |
| `unique_key` | — | Business key identifying the entity |
| `updated_at` | — | Timestamp column (required for timestamp strategy) |
| `check_cols` | `all` | Columns to monitor for changes (check strategy) |
| `invalidate_hard_deletes` | `False` | Set `dbt_valid_to` when source row disappears |

## Example Usage

```bash
# Run snapshots (separate from dbt run)
dbt snapshot

# Or run everything in DAG order
dbt build  # runs snapshots, models, tests in order

# Query point-in-time state
select * from snapshots.snap_customers
where customer_id = 'C-123'
  and '2025-06-15' between dbt_valid_from and coalesce(dbt_valid_to, '9999-12-31')
```

## See Also

- [incremental-model](../patterns/incremental-model.md)
- [model-types](../concepts/model-types.md)
