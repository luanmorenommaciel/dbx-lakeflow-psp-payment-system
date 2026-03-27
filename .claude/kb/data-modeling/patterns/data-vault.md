# Data Vault

> **Purpose**: Data Vault 2.0 — Hubs, Links, Satellites, hash keys, loading patterns, AutomateDV, lakehouse integration
> **Confidence**: 0.92
> **MCP Validated**: 2026-03-26

## Overview

Data Vault 2.0 separates business keys (Hubs), relationships (Links), and descriptive attributes (Satellites). It excels at integrating data from many sources with full history tracking. Hash keys enable parallel loading and deterministic joins. In 2025+, automation tools like AutomateDV (dbt package) and Coalesce have made Data Vault practical at enterprise scale by generating Hub/Link/Sat loading SQL from metadata definitions.

## The Pattern

```sql
-- ============================================================
-- Data Vault: Customer-Order Domain
-- ============================================================

-- Hub: Business entity identified by natural key
CREATE TABLE hub_customer (
    hub_customer_hk   CHAR(32) PRIMARY KEY,     -- MD5/SHA hash of customer_id
    customer_id       VARCHAR(36) NOT NULL,      -- business key
    load_timestamp    TIMESTAMP NOT NULL,
    record_source     VARCHAR(100) NOT NULL       -- 'crm_system', 'web_app'
);

CREATE TABLE hub_order (
    hub_order_hk      CHAR(32) PRIMARY KEY,
    order_id          VARCHAR(36) NOT NULL,
    load_timestamp    TIMESTAMP NOT NULL,
    record_source     VARCHAR(100) NOT NULL
);

-- Link: Relationship between business entities
CREATE TABLE link_customer_order (
    link_customer_order_hk  CHAR(32) PRIMARY KEY,  -- hash of (customer_id + order_id)
    hub_customer_hk         CHAR(32) NOT NULL REFERENCES hub_customer(hub_customer_hk),
    hub_order_hk            CHAR(32) NOT NULL REFERENCES hub_order(hub_order_hk),
    load_timestamp          TIMESTAMP NOT NULL,
    record_source           VARCHAR(100) NOT NULL
);

-- Satellite: Descriptive attributes with history
CREATE TABLE sat_customer_details (
    hub_customer_hk   CHAR(32) NOT NULL REFERENCES hub_customer(hub_customer_hk),
    load_timestamp    TIMESTAMP NOT NULL,
    hash_diff         CHAR(32) NOT NULL,          -- hash of all attributes (change detection)
    customer_name     VARCHAR(200) NOT NULL,
    email             VARCHAR(200),
    segment           VARCHAR(50),
    region            VARCHAR(100),
    record_source     VARCHAR(100) NOT NULL,
    PRIMARY KEY (hub_customer_hk, load_timestamp)
);

CREATE TABLE sat_order_details (
    hub_order_hk      CHAR(32) NOT NULL REFERENCES hub_order(hub_order_hk),
    load_timestamp    TIMESTAMP NOT NULL,
    hash_diff         CHAR(32) NOT NULL,
    order_date        DATE NOT NULL,
    total_amount      DECIMAL(12,2) NOT NULL,
    status            VARCHAR(20) NOT NULL,
    record_source     VARCHAR(100) NOT NULL,
    PRIMARY KEY (hub_order_hk, load_timestamp)
);

-- Loading pattern: Hub load (idempotent)
INSERT INTO hub_customer (hub_customer_hk, customer_id, load_timestamp, record_source)
SELECT
    MD5(customer_id) AS hub_customer_hk,
    customer_id,
    CURRENT_TIMESTAMP,
    'crm_system'
FROM staging_customers s
WHERE NOT EXISTS (
    SELECT 1 FROM hub_customer h
    WHERE h.hub_customer_hk = MD5(s.customer_id)
);

-- Loading pattern: Satellite load (change detection via hash_diff)
INSERT INTO sat_customer_details
SELECT
    MD5(s.customer_id),
    CURRENT_TIMESTAMP,
    MD5(CONCAT(s.customer_name, s.email, s.segment, s.region)),
    s.customer_name, s.email, s.segment, s.region,
    'crm_system'
FROM staging_customers s
WHERE NOT EXISTS (
    SELECT 1 FROM sat_customer_details sat
    WHERE sat.hub_customer_hk = MD5(s.customer_id)
      AND sat.hash_diff = MD5(CONCAT(s.customer_name, s.email, s.segment, s.region))
);
```

## Quick Reference

| Component | Contains | Key | Cardinality |
|-----------|----------|-----|-------------|
| Hub | Business key | Hash of natural key | One row per entity |
| Link | Relationship | Hash of parent HKs | One row per relationship |
| Satellite | Attributes + history | Hub HK + load_timestamp | Multiple rows per entity |
| PIT (Point-in-Time) | Snapshot helper | Hub HK + snapshot_date | Pre-joined satellites |

## Common Mistakes

### Wrong

```sql
-- Satellite without hash_diff — reloads identical rows
INSERT INTO sat_customer_details
SELECT MD5(customer_id), CURRENT_TIMESTAMP, name, email FROM staging;
```

### Correct

```sql
-- hash_diff prevents duplicate satellite records
INSERT INTO sat_customer_details
SELECT MD5(customer_id), CURRENT_TIMESTAMP,
       MD5(CONCAT(name, email)),  -- change detection
       name, email
FROM staging s
WHERE NOT EXISTS (
    SELECT 1 FROM sat_customer_details sat
    WHERE sat.hub_customer_hk = MD5(s.customer_id)
      AND sat.hash_diff = MD5(CONCAT(s.name, s.email))
);
```

## AutomateDV: Metadata-Driven Loading (2025+ Best Practice)

```yaml
# dbt AutomateDV: metadata-driven hub definition
# models/raw_vault/hubs/hub_customer.yml
hub_customer:
  src_pk: CUSTOMER_HK
  src_nk: CUSTOMER_ID
  src_ldts: LOAD_DATETIME
  src_source: RECORD_SOURCE
  source_model: stg_crm_customers
```

```sql
-- AutomateDV dbt model: hub_customer.sql
{{ automate_dv.hub(
    src_pk='CUSTOMER_HK',
    src_nk='CUSTOMER_ID',
    src_ldts='LOAD_DATETIME',
    src_source='RECORD_SOURCE',
    source_model='stg_crm_customers'
) }}
```

### Data Vault in the Lakehouse

| Platform | Data Vault Support | Tooling |
|----------|-------------------|---------|
| Databricks | Delta Lake tables for Hubs/Links/Sats | AutomateDV + dbt |
| Snowflake | Native tables, HASH function support | AutomateDV + dbt / Coalesce |
| BigQuery | Partitioned tables, SHA256 hashing | AutomateDV + dbt |
| Iceberg + Spark | Iceberg tables with merge support | AutomateDV + dbt-spark |

### Raw Vault vs Business Vault

| Layer | Purpose | Transformation |
|-------|---------|---------------|
| Raw Vault | Source-faithful, no business rules | Hash keys, load metadata only |
| Business Vault | Derived relationships, computed satellites | Business logic applied |
| Information Mart | Consumption-ready star schemas | Dimensional model from vault |

## Related

- [dimensional-modeling](../concepts/dimensional-modeling.md)
- [star-schema](star-schema.md)
- [scd-types](../concepts/scd-types.md)
