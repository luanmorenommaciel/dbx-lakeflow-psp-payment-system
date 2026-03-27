# Analytics Engineering

> **Purpose**: Modern analytics engineering tools — Malloy, Evidence.dev, Observable Framework, code-first analytics philosophy
> **MCP Validated**: 2026-03-26

## Overview

Analytics engineering is evolving beyond dbt + Looker. A new generation of code-first tools treats analytics artifacts (dashboards, reports, semantic models) as version-controlled code rather than drag-and-drop configurations.

## Key Concepts

### Malloy — Semantic Modeling Language

Malloy is a language for data analysis that replaces SQL for analytical queries. It separates the semantic model (relationships, measures, dimensions) from the query, making analytical logic reusable.

```malloy
source: orders is duckdb.table('orders.parquet') extend {
  dimension: order_month is order_date.month
  measure:
    total_revenue is net_amount.sum()
    avg_order_value is net_amount.avg()
    order_count is count()

  join_one: customers is duckdb.table('customers.parquet')
    on customer_id = customers.customer_id
}

-- Query the semantic model
run: orders -> {
  group_by: customers.segment, order_month
  aggregate: total_revenue, order_count
  order_by: total_revenue desc
}
```

**When to use**: Complex analytical queries with reusable dimensions/measures, replacing LookML or dbt metrics.

### Evidence.dev — Code-First BI

Evidence turns markdown + SQL into static reporting sites. No drag-and-drop, no server — just code.

```markdown
# Revenue Dashboard

```sql revenue_by_month
SELECT DATE_TRUNC('month', order_date) AS month,
       SUM(net_amount) AS revenue
FROM orders
GROUP BY 1
ORDER BY 1
```

<LineChart data={revenue_by_month} x=month y=revenue />

Revenue this month is **<Value data={revenue_by_month} column=revenue row=last />**.
```

**When to use**: Internal dashboards, data team reports, investor updates — anywhere you want version-controlled, reproducible BI.

### Observable Framework — Reactive Data Apps

Observable creates reactive notebooks and data apps with JavaScript/TypeScript. Ideal for exploratory analysis and interactive visualizations.

**When to use**: Interactive data exploration, public-facing data journalism, custom visualizations beyond standard charts.

### Code-First Philosophy

| Traditional BI | Code-First Analytics |
|----------------|---------------------|
| Drag-and-drop dashboards | SQL/markdown files in git |
| Proprietary formats | Open standards (SQL, markdown, YAML) |
| Click to deploy | CI/CD pipeline deploy |
| Per-seat licensing | Open source or usage-based |
| Semantic layer in BI tool | Semantic layer in code (Malloy, dbt metrics) |

## When to Use

| Tool | Best For | Stack |
|------|----------|-------|
| **Malloy** | Reusable semantic models, replacing LookML | DuckDB, BigQuery, Postgres |
| **Evidence.dev** | Version-controlled dashboards, static reports | Any SQL database |
| **Observable** | Interactive exploration, custom viz | JavaScript, any data source |
| **dbt + Looker** | Enterprise BI with governance | Cloud warehouses |

## Trade-offs

| Approach | Pros | Cons |
|----------|------|------|
| Code-first | Version control, reproducible, reviewable | Steeper learning curve for non-engineers |
| Traditional BI | Accessible to non-technical users | Vendor lock-in, hard to version control |
| Hybrid | Best of both worlds | More tools to maintain |

## See Also

- [duckdb](../concepts/duckdb.md)
- [sqlmesh](../concepts/sqlmesh.md)
- [local-first-analytics](../patterns/local-first-analytics.md)
