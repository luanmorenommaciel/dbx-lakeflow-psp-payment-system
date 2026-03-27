# Feature Stores

> **Purpose**: Online/offline store architecture, Feast, Tecton, Hopsworks, point-in-time joins, feature serving
> **Confidence**: 0.90
> **MCP Validated**: 2026-03-26

## Overview

Feature stores solve the training-serving skew problem by providing a single system for defining, storing, and serving ML features. They maintain two stores: an **offline store** (data lake/warehouse) for historical training data with point-in-time correctness, and an **online store** (Redis, DynamoDB) for low-latency inference serving. In 2026, the landscape includes Feast (dominant OSS), Tecton (enterprise), Hopsworks (feature platform with native vector support), and cloud-native options (Databricks Feature Store, SageMaker Feature Store). Key 2026 trends: vector feature support, streaming feature computation, and declarative feature definitions.

## The Concept

```python
# Feast Feature Store Architecture
from datetime import timedelta
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String

# 1. Define the entity (join key for features)
customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    description="Unique customer identifier",
)

# 2. Define the data source (offline store)
customer_stats_source = FileSource(
    path="s3://features/customer_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

# 3. Define the feature view (maps source -> features)
customer_stats_fv = FeatureView(
    name="customer_stats",
    entities=[customer],
    ttl=timedelta(days=7),  # feature freshness SLA
    schema=[
        Field(name="total_orders", dtype=Int64),
        Field(name="avg_order_value", dtype=Float32),
        Field(name="lifetime_value", dtype=Float32),
        Field(name="last_purchase_days", dtype=Int64),
        Field(name="preferred_category", dtype=String),
    ],
    source=customer_stats_source,
    online=True,  # materialize to online store
)
```

```text
Feature Store Architecture

TRAINING (Offline Path)                    SERVING (Online Path)
========================                   ========================

  Feature           Point-in-Time             Online Store
  Definitions  -->  Join (get_historical  --> (Redis / DynamoDB)
  (Python)          _features)                  |
     |                   |                      v
     v                   v                 get_online_features()
  Offline Store     Training Dataset        < 10ms p99 latency
  (Parquet, BQ,     (no future leak)
   Redshift)

  materialize-incremental: Offline --> Online (scheduled)
```

## Quick Reference

| Concept | Description | Critical For |
|---------|-------------|-------------|
| Point-in-time join | Join features as-of entity timestamp | Preventing data leakage |
| Materialization | Copy features from offline to online store | Serving freshness |
| TTL (time-to-live) | Max age before feature is stale | Feature freshness SLA |
| Feature service | Group of feature views for a model | Serving contract |
| On-demand features | Computed at request time (transforms) | Real-time derived features |
| Push source | Streaming feature ingestion | Low-latency feature updates |

| Platform | Online Store | Offline Store | Streaming | Vector | Best For |
|----------|-------------|---------------|-----------|--------|----------|
| Feast | Redis, DynamoDB | Parquet, BQ | Basic (push) | No | OSS, flexible |
| Tecton | Managed (<5ms) | Managed | Full | No | Enterprise |
| Hopsworks | RonDB (<5ms) | Hudi/Delta | Full | Native | Feature platform |
| Databricks FS | Databricks | Delta Lake | Full | Yes | Databricks shops |
| SageMaker FS | DynamoDB | S3/Parquet | Full | No | AWS shops |

## Common Mistakes

### Wrong
```python
# Training without point-in-time join -- data leakage!
features = db.query("""
    SELECT t.*, f.*
    FROM training_events t
    JOIN feature_table f ON t.customer_id = f.customer_id
    -- Missing: AND f.event_timestamp <= t.event_timestamp
""")
```

### Correct
```python
# Point-in-time correct training dataset via Feast
from feast import FeatureStore

store = FeatureStore(repo_path="feast_repo/")
training_df = store.get_historical_features(
    entity_df=entity_df,  # must have customer_id + event_timestamp
    features=[
        "customer_stats:total_orders",
        "customer_stats:avg_order_value",
        "customer_stats:lifetime_value",
    ],
).to_df()
# Feast joins features as-of each entity's event_timestamp
# No future data leakage
```

## Related

- [embedding-pipelines](../concepts/embedding-pipelines.md)
- [llmops-patterns](../concepts/llmops-patterns.md)
- [feature-engineering](../patterns/feature-engineering.md)
