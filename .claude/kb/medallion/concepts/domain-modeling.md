# Domain Modeling

> **Purpose**: Domain-driven design applied to lakehouse organization for data mesh and bounded contexts
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

Domain modeling in the Medallion Architecture organizes data assets by business domains
rather than purely technical layers. Inspired by Domain-Driven Design (DDD) and Data Mesh,
this approach assigns ownership of Bronze/Silver/Gold tables to specific business domains
(e.g., sales, inventory, finance), enabling decentralized governance while maintaining
the layered quality progression.

## The Pattern

```python
# Domain-oriented database/schema organization
DOMAIN_CONFIG = {
    "sales": {
        "bronze": "bronze_sales",      # raw sales data
        "silver": "silver_sales",      # cleansed sales data
        "gold": "gold_sales",          # sales KPIs and dims
        "owner": "sales-data-team",
        "tables": {
            "bronze": ["raw_orders", "raw_returns", "raw_payments"],
            "silver": ["cleansed_orders", "cleansed_returns", "cleansed_payments"],
            "gold": ["dim_customers", "fact_orders", "agg_daily_revenue"],
        }
    },
    "inventory": {
        "bronze": "bronze_inventory",
        "silver": "silver_inventory",
        "gold": "gold_inventory",
        "owner": "supply-chain-team",
        "tables": {
            "bronze": ["raw_stock_levels", "raw_shipments"],
            "silver": ["cleansed_stock", "cleansed_shipments"],
            "gold": ["dim_products", "dim_warehouses", "fact_stock_movements"],
        }
    },
}
```

## Database Naming Convention

| Pattern | Example | When to Use |
|---------|---------|-------------|
| `{layer}_{domain}` | `bronze_sales` | Default: layer-first organization |
| `{domain}_{layer}` | `sales_bronze` | Domain-first: data mesh focus |
| `{domain}.{layer}_{entity}` | `sales.silver_orders` | Unity Catalog with schemas |

## Unity Catalog Hierarchy

```text
catalog (environment)
  -- schema (layer_domain)
       -- table (entity)

Example:
  production
    -- bronze_sales
         -- raw_orders
         -- raw_returns
    -- silver_sales
         -- cleansed_orders
    -- gold_sales
         -- fact_orders
         -- dim_customers
         -- agg_daily_revenue
```

## Cross-Domain Joins

```sql
-- Gold tables can join across domains
-- Each domain owns its own dim/fact tables
-- Shared dimensions use a "shared" or "master" domain

CREATE OR REPLACE TABLE gold_analytics.agg_revenue_by_product AS
SELECT
    p.product_category,
    p.product_name,
    SUM(o.total_amount) AS total_revenue,
    COUNT(DISTINCT o.customer_id) AS unique_buyers
FROM gold_sales.fact_orders o
JOIN gold_inventory.dim_products p ON o.product_id = p.product_id
GROUP BY 1, 2;
```

## Data Contracts Between Domains

```yaml
# data-contract: silver_sales.cleansed_orders
domain: sales
owner: sales-data-team
version: "2.1"
sla:
  freshness: "1 hour"
  completeness: ">= 99.5%"
  availability: "99.9%"
schema:
  - name: order_id
    type: STRING
    nullable: false
    primary_key: true
  - name: customer_id
    type: STRING
    nullable: false
  - name: amount
    type: DECIMAL(10,2)
    nullable: false
    check: "> 0"
consumers:
  - gold_sales.fact_orders
  - gold_analytics.agg_revenue_by_product
```

## Common Mistakes

### Wrong -- Monolithic Namespace

```sql
-- Everything in one database with no domain separation
CREATE TABLE lakehouse.orders ...
CREATE TABLE lakehouse.products ...
CREATE TABLE lakehouse.revenue_kpi ...
```

### Correct -- Domain-Separated Namespaces

```sql
-- Clear domain ownership and layer separation
CREATE TABLE bronze_sales.raw_orders ...
CREATE TABLE silver_sales.cleansed_orders ...
CREATE TABLE gold_sales.fact_orders ...
CREATE TABLE gold_inventory.dim_products ...
```

## Related

- [Gold Layer](../concepts/gold-layer.md)
- [Layer Transitions](../patterns/layer-transitions.md)
- [Data Quality Gates](../patterns/data-quality-gates.md)
