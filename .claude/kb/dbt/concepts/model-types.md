# Model Types

> **Purpose**: Staging, intermediate, mart, and snapshot model conventions
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

dbt models are organized in layers: **staging** (1:1 with sources), **intermediate** (business logic), and **marts** (consumer-facing facts and dimensions). Each layer has naming conventions and materialization defaults that enforce separation of concerns.

## The Concept

```sql
-- models/staging/stripe/stg_stripe__payments.sql
-- Staging: 1:1 with source, rename + cast, no business logic

with source as (
    select * from {{ source('stripe', 'payments') }}
),

renamed as (
    select
        id                          as payment_id,
        order_id                    as order_id,
        cast(amount as decimal(12,2)) / 100 as amount_dollars,
        currency                    as currency_code,
        status                      as payment_status,
        created::timestamp          as created_at
    from source
)

select * from renamed
```

## Quick Reference

| Layer | Prefix | Materialization | Business Logic |
|-------|--------|----------------|----------------|
| Staging | `stg_{source}__{entity}` | view | None — rename, cast, dedupe only |
| Intermediate | `int_{entity}_{verb}` | ephemeral/view | Yes — joins, filters, calculations |
| Facts | `fct_{event}` | table/incremental | Grain-level measurable events |
| Dimensions | `dim_{entity}` | table | Descriptive attributes, SCD |
| Snapshots | `snap_{entity}` | snapshot | SCD Type 2 historical tracking |

## Common Mistakes

### Wrong

```sql
-- Staging model with business logic and joins
select
    p.id,
    p.amount * e.exchange_rate as amount_usd,  -- business logic in staging!
    c.name as customer_name                      -- join in staging!
from {{ source('stripe', 'payments') }} p
join {{ ref('exchange_rates') }} e on p.currency = e.code
join {{ ref('customers') }} c on p.customer_id = c.id
```

### Correct

```sql
-- Staging: only rename and cast
select
    id as payment_id,
    amount,
    currency,
    customer_id,
    created::timestamp as created_at
from {{ source('stripe', 'payments') }}

-- Business logic goes in intermediate or mart models
```

## Related

- [incremental-strategies](../concepts/incremental-strategies.md)
- [incremental-model](../patterns/incremental-model.md)
