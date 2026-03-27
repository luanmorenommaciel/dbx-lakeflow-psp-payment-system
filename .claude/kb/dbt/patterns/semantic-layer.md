# Semantic Layer

> **Purpose**: Centralized metric definitions via MetricFlow — semantic models, measures, dimensions
> **MCP Validated**: 2026-03-26 | Updated with AI-readiness and governance patterns

## When to Use

- Multiple BI tools or consumers need consistent metric definitions
- Business users define metrics differently across teams ("revenue" = gross vs net)
- You want a single source of truth for KPIs across dashboards, reports, and APIs
- Integrating with the dbt Semantic Layer Gateway for low-latency queries
- **AI agents and LLMs** need consistent, trusted metric definitions (semantic inconsistency causes hallucinated answers)
- Production governance: the semantic layer acts as a contract between data domains

## Implementation

```yaml
# models/marts/finance/_finance__semantic_models.yml
# Define semantic model on top of an existing dbt model

semantic_models:
  - name: orders
    defaults:
      agg_time_dimension: order_date
    description: "Order facts for revenue and volume metrics"
    model: ref('fct_orders')

    entities:
      - name: order_id
        type: primary
      - name: customer_id
        type: foreign

    measures:
      - name: order_count
        agg: count
        expr: order_id
      - name: gross_revenue
        agg: sum
        expr: order_total
      - name: net_revenue
        agg: sum
        expr: net_revenue
      - name: average_order_value
        agg: average
        expr: order_total

    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
      - name: order_status
        type: categorical
      - name: payment_method
        type: categorical
      - name: country
        type: categorical
```

```yaml
# models/marts/finance/_finance__metrics.yml
# Define derived and ratio metrics

metrics:
  - name: revenue
    description: "Total net revenue"
    type: simple
    type_params:
      measure: net_revenue

  - name: revenue_per_customer
    description: "Average revenue per unique customer"
    type: derived
    type_params:
      expr: revenue / unique_customers
      metrics:
        - name: revenue
        - name: unique_customers

  - name: unique_customers
    description: "Count of distinct customers"
    type: simple
    type_params:
      measure:
        name: customer_count
        filter: "{{ Dimension('order__order_status') }} = 'completed'"
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `agg_time_dimension` | — | Default time dimension for time-series queries |
| `type_params.time_granularity` | `day` | Finest grain for time dimension |
| `type` (metric) | — | `simple`, `derived`, `ratio`, `cumulative` |
| `filter` | — | MetricFlow filter syntax for metric-level filtering |

## Production Governance Patterns

| Pattern | Description |
|---------|-------------|
| **Domain ownership** | Each domain team owns its semantic models and metrics |
| **Naming conventions** | Prefix metrics by domain: `finance__revenue`, `marketing__cac` |
| **Testing** | Add data tests to underlying models; validate metric freshness |
| **Join graph design** | Keep entity relationships explicit; avoid fan-out joins |
| **Performance tuning** | Pre-aggregate high-cardinality dimensions; use `agg_time_dimension` |
| **AI readiness** | Consistent definitions prevent LLM hallucination on business metrics |

## Example Usage

```sql
-- Query the Semantic Layer (MetricFlow CLI or API)
mf query --metrics revenue,order_count \
         --group-by order_date__month,country \
         --where "order_date >= '2025-01-01'"

-- Output: consistent metrics regardless of which tool queries them
```

```yaml
# Expose metrics to downstream consumers
exposures:
  - name: executive_dashboard
    type: dashboard
    maturity: high
    url: https://bi.company.com/exec-dashboard
    depends_on:
      - ref('fct_orders')
    owner:
      name: Finance Analytics
      email: finance-data@company.com
```

## See Also

- [mesh-architecture](../concepts/mesh-architecture.md)
- [model-types](../concepts/model-types.md)
