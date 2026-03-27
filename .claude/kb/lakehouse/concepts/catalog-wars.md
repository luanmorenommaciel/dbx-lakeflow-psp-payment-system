# Catalog Wars

> **Purpose**: Catalog comparison — Unity Catalog 0.3.1, Gravitino 1.1, Nessie, Polaris TLP 1.3, decision matrix
> **Confidence**: 0.92
> **MCP Validated**: 2026-03-26

## Overview

Data catalogs manage metadata, access control, and table discovery across lakehouse engines. The landscape shifted significantly in 2025-2026: Apache Polaris graduated to TLP (Feb 2026), Gravitino reached 1.0 GA with AI-native metadata (Sep 2025), and Unity Catalog OSS hit 0.3.1 with managed Delta tables. The "catalog wars" are converging toward REST catalog standards (Iceberg REST spec) as the common protocol.

## The Concept

```yaml
# Gravitino: Apache multi-engine catalog (YAML config)
gravitino:
  server:
    host: 0.0.0.0
    port: 8090
  catalogs:
    - name: lakehouse
      type: iceberg
      properties:
        catalog-backend: rest
        uri: http://rest-catalog:8181
        warehouse: s3://my-bucket/warehouse
    - name: hive_legacy
      type: hive
      properties:
        metastore.uris: thrift://hive-metastore:9083
```

```sql
-- Nessie: Git-like branching for data lakes
-- Create a branch for development
CREATE BRANCH dev_feature IN nessie;

-- Work on the branch (isolated from main)
USE REFERENCE dev_feature IN nessie;
INSERT INTO orders VALUES ('new-order', 'cust-1', '2026-03-26', 99.99);

-- Merge when ready (atomic)
MERGE BRANCH dev_feature INTO main IN nessie;
```

## Quick Reference

| Catalog | Backed By | Version | Format Support | Open Source | Multi-Engine | Governance |
|---------|----------|---------|---------------|------------|-------------|------------|
| Unity Catalog | Databricks | OSS 0.3.1 | Delta, Iceberg (UniForm) | OSS fork | Spark, DuckDB, Trino | ABAC + tags + quality monitoring |
| Apache Polaris | Apache (TLP) | 1.3.0 | Iceberg + generic tables (Delta, Hudi) | Full ASF | Any (REST standard) | RBAC + OPA (1.3) |
| Apache Gravitino | Apache (TLP) | 1.1.0 | Iceberg, Hive, JDBC, Kafka, Lance | Full ASF | Any engine | Unified RBAC + OpenLineage |
| Nessie | Dremio | 0.95+ | Iceberg | Yes | Spark, Trino, Flink | Git-based branching |
| Hive Metastore | Apache | 3.x | Hive, Iceberg (adapter) | Yes | All | Basic (legacy) |
| AWS Glue | AWS | v4 | Iceberg, Delta, Hive | No | Spark, Trino, Athena | IAM |

| Decision Factor | Weight | Best Option |
|----------------|--------|-------------|
| Multi-engine flexibility | High | Polaris (REST standard), Gravitino |
| Git-like branching | High | Nessie |
| Databricks ecosystem | High | Unity Catalog |
| Snowflake + Iceberg | High | Polaris |
| Minimal vendor lock-in | High | Polaris (TLP), Gravitino (TLP) |
| AI/ML metadata (models, vectors) | High | Gravitino 1.1 (Model Catalog + Lance) |
| Managed service | Medium | AWS Glue, Unity (Databricks-managed) |
| Non-Iceberg format federation | Medium | Polaris 1.3 (generic tables), Gravitino |

## 2025-2026 Milestones

| Event | Date | Significance |
|-------|------|-------------|
| Gravitino 1.0 GA | Sep 2025 | First stable release, unified RBAC, metadata-driven actions |
| Gravitino 1.1 | Dec 2025 | Lance REST service for AI, multi-cluster filesets |
| Polaris 1.3 | Jan 2026 | Generic tables (non-Iceberg), OPA auth, Iceberg metrics |
| Polaris TLP graduation | Feb 2026 | Full Apache TLP status, vendor-independent governance |
| Unity Catalog 0.3.1 | Feb 2026 | Managed Delta tables, improved credential renewal |
| Delta Lake 4.1 | Mar 2026 | Catalog-managed tables GA, server-side planning preview |

## Common Mistakes

### Wrong

```text
Using Hive Metastore as the long-term catalog for Iceberg tables.
HMS lacks: fine-grained RBAC, partition evolution awareness, REST API standard.
```

### Correct

```text
Migrate to REST Catalog (Polaris, Gravitino) for production Iceberg.
Keep HMS as a read-only bridge during migration for legacy Hive consumers.
```

## Related

- [iceberg-v3](iceberg-v3.md)
- [delta-lake](delta-lake.md)
- [catalog-setup pattern](../patterns/catalog-setup.md)
