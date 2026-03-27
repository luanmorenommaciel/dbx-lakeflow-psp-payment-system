# Catalog Setup

> **Purpose**: Gravitino config, Unity Catalog CLI, Nessie branching, catalog federation
> **Confidence**: 0.85
> **MCP Validated**: 2026-03-26

## Overview

Setting up catalogs for multi-engine lakehouse environments. Covers Gravitino (Apache federation), Unity Catalog (Databricks), Nessie (git-like branching), and Polaris (Iceberg REST). Each serves different use cases in the catalog ecosystem.

## The Pattern

```yaml
# ============================================================
# Gravitino: Multi-engine catalog federation
# ============================================================
# gravitino-server.conf
gravitino.server.webserver.host: 0.0.0.0
gravitino.server.webserver.httpPort: 8090
gravitino.auxService.names: iceberg-rest
gravitino.iceberg-rest.classpath: catalogs/lakehouse-iceberg/libs
gravitino.iceberg-rest.host: 0.0.0.0
gravitino.iceberg-rest.port: 9001
```

```python
# Gravitino: Create catalog via Python SDK
from gravitino import GravitinoClient, Catalog

client = GravitinoClient(uri="http://gravitino:8090")
metalake = client.load_metalake("production")

catalog = metalake.create_catalog(
    name="analytics_iceberg",
    catalog_type=Catalog.Type.RELATIONAL,
    provider="lakehouse-iceberg",
    properties={
        "catalog-backend": "rest",
        "uri": "http://iceberg-rest:8181",
        "warehouse": "s3://warehouse/iceberg",
    }
)
```

```sql
-- ============================================================
-- Unity Catalog: Namespace hierarchy
-- ============================================================
-- Three-level namespace: catalog.schema.table
CREATE CATALOG analytics;
CREATE SCHEMA analytics.bronze;
CREATE SCHEMA analytics.silver;
CREATE SCHEMA analytics.gold;

-- External location for storage
CREATE EXTERNAL LOCATION bronze_storage
URL 's3://data-lake/bronze/'
WITH (STORAGE CREDENTIAL bronze_cred);

-- Grant access (RBAC)
GRANT USE CATALOG ON CATALOG analytics TO `data-engineers`;
GRANT USE SCHEMA ON SCHEMA analytics.bronze TO `data-engineers`;
GRANT SELECT ON SCHEMA analytics.gold TO `analysts`;
```

```sql
-- ============================================================
-- Nessie: Git-like branching for data lakes
-- ============================================================
-- Create feature branch
CREATE BRANCH feature_new_model IN nessie;

-- Switch to branch
USE REFERENCE feature_new_model IN nessie;

-- Make changes (isolated from main)
CREATE TABLE IF NOT EXISTS analytics.stg_orders (...) USING iceberg;
INSERT INTO analytics.stg_orders SELECT * FROM raw.orders;

-- Review changes (diff)
-- Via Nessie REST API: GET /api/v2/trees/diff/main...feature_new_model

-- Merge to main (atomic)
MERGE BRANCH feature_new_model INTO main IN nessie;
DROP BRANCH feature_new_model IN nessie;
```

## Polaris Setup (Apache TLP, REST catalog standard)

```yaml
# Polaris: Apache Iceberg REST catalog (docker-compose)
services:
  polaris:
    image: apache/polaris:1.3.0
    ports:
      - "8181:8181"
    environment:
      POLARIS_AUTH_TYPE: opa                    # OPA integration (v1.3)
      POLARIS_OPA_URL: http://opa:8181/v1/data
    volumes:
      - polaris-data:/data
```

```sql
-- Polaris: register catalog in Spark
spark.sql.catalog.polaris = org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.polaris.type = rest
spark.sql.catalog.polaris.uri = http://polaris:8181/api/catalog
spark.sql.catalog.polaris.warehouse = my_warehouse

-- Polaris 1.3: generic tables (non-Iceberg formats)
-- Register Delta/Hudi tables alongside Iceberg in single catalog
```

## Quick Reference

| Catalog | Setup Complexity | Multi-Engine | Branching | Governance | AI/ML |
|---------|-----------------|-------------|-----------|------------|-------|
| Gravitino 1.1 | Medium (YAML + SDK) | Excellent | No | Unified RBAC + OpenLineage | Model Catalog + Lance REST |
| Unity Catalog 0.3.1 | Low (Databricks) | Good (expanding) | No | ABAC + tags + quality | Model registry, feature store |
| Nessie | Medium (REST server) | Good | Yes (git-like) | Basic | No |
| Polaris 1.3 (TLP) | Low (REST) | Excellent | No | RBAC + OPA | Iceberg metrics |
| AWS Glue | Low (managed) | Good (AWS) | No | IAM-based | Glue ML |

## Related

- [catalog-wars concept](../concepts/catalog-wars.md)
- [iceberg-operations](iceberg-operations.md)
- [delta-operations](delta-operations.md)
