# Incremental Model

> **Purpose**: Production-ready incremental model with merge strategy and late-arriving fact handling
> **MCP Validated**: 2026-03-26 | Updated with --sample flag (v1.10+) and microbatch references

## When to Use

- Table has more than 1M rows and grows daily
- Source data is mutable (rows can be updated after initial insert)
- Pipeline SLA requires processing only new/changed data
- Full refresh would exceed compute budget or time window

## Implementation

```sql
-- models/marts/finance/fct_orders.sql
-- Incremental model: merge strategy with late-arriving fact handling

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
        cluster_by=['order_date'],
        tags=['daily', 'finance']
    )
}}

with source_orders as (
    select
        order_id,
        customer_id,
        product_id,
        order_date,
        quantity,
        unit_price,
        quantity * unit_price           as order_total,
        discount_amount,
        (quantity * unit_price) - coalesce(discount_amount, 0)
                                        as net_revenue,
        currency_code,
        payment_method,
        status                          as order_status,
        shipped_at,
        delivered_at,
        updated_at,
        created_at
    from {{ ref('stg_stripe__orders') }}

    {% if is_incremental() %}
        -- Process rows updated since last run
        -- Use 3-day lookback for late-arriving facts
        where updated_at > (
            select dateadd(day, -3, max(updated_at))
            from {{ this }}
        )
    {% endif %}
),

-- Deduplicate in case of multiple updates within the window
deduplicated as (
    select
        *,
        row_number() over (
            partition by order_id
            order by updated_at desc
        ) as row_num
    from source_orders
)

select
    {{ dbt_utils.generate_surrogate_key(['order_id']) }} as order_sk,
    order_id,
    customer_id,
    product_id,
    order_date,
    quantity,
    unit_price,
    order_total,
    discount_amount,
    net_revenue,
    currency_code,
    payment_method,
    order_status,
    shipped_at,
    delivered_at,
    updated_at,
    created_at
from deduplicated
where row_num = 1
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `unique_key` | None | Column(s) for merge matching — required for merge/delete+insert |
| `incremental_strategy` | `merge` | One of: append, merge, delete+insert, insert_overwrite |
| `on_schema_change` | `ignore` | How to handle new columns: ignore, append_new_columns, sync_all_columns, fail |
| `cluster_by` | None | Clustering columns for query performance (Snowflake/BigQuery) |
| `incremental_predicates` | None | Additional merge predicates for partition pruning |

## Example Usage

```bash
# Normal incremental run
dbt run --select fct_orders

# Full refresh (rebuilds from scratch)
dbt run --select fct_orders --full-refresh

# Run with upstream dependencies
dbt run --select +fct_orders

# Sample mode (v1.10+) — validate with subset of data, saves time and cost
dbt run --select fct_orders --sample="3 days"

# Sample with static time range
dbt run --select fct_orders --sample="{'start': '2026-01-01', 'end': '2026-01-07'}"
```

> **Tip (v1.10+)**: The `--sample` flag requires `event_time` to be set on the model for
> time-based sampling. It generates filtered refs and sources so you can validate
> outputs without building the entire model. Not supported for Python models.

> **Tip (v1.9+)**: For large time-series data, consider the `microbatch` incremental
> strategy instead of merge. It processes data in time batches (hour/day/month)
> without requiring `is_incremental()` logic. See [incremental-strategies](../concepts/incremental-strategies.md).

## See Also

- [incremental-strategies](../concepts/incremental-strategies.md)
- [snapshot-scd2](../patterns/snapshot-scd2.md)
