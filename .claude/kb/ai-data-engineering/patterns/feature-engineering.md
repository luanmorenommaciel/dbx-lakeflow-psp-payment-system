# Feature Engineering

> **Purpose**: Feast feature store patterns -- entity definitions, feature views, materialization, online serving, and point-in-time joins
> **MCP Validated**: 2026-03-26

## When to Use

- Sharing features across ML models and teams
- Need consistent feature values between training and serving (train/serve skew prevention)
- Managing time-travel joins for training data construction
- Serving low-latency features for real-time inference
- Building a feature catalog with discovery and reuse

## Implementation

```python
"""Feast Feature Store: definitions, materialization, and serving."""

from datetime import timedelta, datetime
from feast import (
    Entity, FeatureView, FeatureService, Field,
    FileSource, OnDemandFeatureView, FeatureStore,
    on_demand_feature_view,
)
from feast.types import Float32, Int64, String

# --- 1. Entity Definitions ---
customer = Entity(
    name="customer_id",
    join_keys=["customer_id"],
    description="Unique customer identifier",
)

order = Entity(
    name="order_id",
    join_keys=["order_id"],
    description="Unique order identifier",
)

# --- 2. Batch Source ---
customer_stats_source = FileSource(
    path="data/customer_stats.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_at",
)

# Alternative: BigQuery source for production
# from feast import BigQuerySource
# customer_stats_source = BigQuerySource(
#     table="project.dataset.customer_stats",
#     timestamp_field="event_timestamp",
# )

# --- 3. Feature View (batch features) ---
customer_stats_fv = FeatureView(
    name="customer_stats",
    entities=[customer],
    schema=[
        Field(name="total_orders", dtype=Int64),
        Field(name="avg_order_value", dtype=Float32),
        Field(name="lifetime_value", dtype=Float32),
        Field(name="days_since_last_order", dtype=Int64),
        Field(name="preferred_category", dtype=String),
    ],
    source=customer_stats_source,
    ttl=timedelta(days=1),  # Feature freshness requirement
    online=True,
    tags={"team": "data-engineering", "version": "v2"},
)

# --- 4. On-Demand Feature View (real-time transforms) ---
@on_demand_feature_view(
    sources=[customer_stats_fv],
    schema=[
        Field(name="is_high_value", dtype=Int64),
        Field(name="order_frequency_score", dtype=Float32),
    ],
)
def customer_derived_features(inputs):
    """Compute derived features at request time."""
    df = inputs["customer_stats"]
    df["is_high_value"] = (df["lifetime_value"] > 1000).astype(int)
    df["order_frequency_score"] = df["total_orders"] / (df["days_since_last_order"] + 1)
    return df

# --- 5. Feature Service (group features for a model) ---
fraud_detection_service = FeatureService(
    name="fraud_detection",
    features=[
        customer_stats_fv[["total_orders", "avg_order_value", "lifetime_value"]],
        customer_derived_features,
    ],
    tags={"model": "fraud-v3"},
)

# --- 6. Materialization (offline -> online store) ---
# CLI: feast materialize 2025-01-01T00:00:00 2026-03-26T00:00:00
store = FeatureStore(repo_path="feature_repo/")
store.materialize(
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2026, 3, 26),
)

# --- 7. Online Serving (low-latency lookups) ---
online_features = store.get_online_features(
    features=[
        "customer_stats:total_orders",
        "customer_stats:avg_order_value",
        "customer_stats:lifetime_value",
    ],
    entity_rows=[{"customer_id": "C-12345"}],
).to_dict()

# --- 8. Point-in-Time Join (training data) ---
import pandas as pd

entity_df = pd.DataFrame({
    "customer_id": ["C-12345", "C-67890", "C-11111"],
    "event_timestamp": pd.to_datetime([
        "2026-01-15", "2026-02-20", "2026-03-10",
    ]),
    "label": [1, 0, 1],  # fraud labels
})

training_df = store.get_historical_features(
    entity_df=entity_df,
    features=fraud_detection_service,
).to_df()

print(training_df.columns.tolist())
# ['customer_id', 'event_timestamp', 'label', 'total_orders',
#  'avg_order_value', 'lifetime_value', 'is_high_value', 'order_frequency_score']
```

## Configuration

| Parameter | Description | Typical Value |
|-----------|-------------|---------------|
| `ttl` | Feature freshness (time-to-live) | `timedelta(days=1)` |
| `online` | Materialize to online store | `True` for serving features |
| `tags` | Metadata for discovery | `{"team": "...", "version": "..."}` |
| Online store | Low-latency backend | Redis, DynamoDB, SQLite |
| Offline store | Batch/historical backend | BigQuery, Redshift, file |
| Registry | Metadata storage | Local file, S3, GCS |

## Example Usage

```bash
# Initialize a Feast project
feast init feature_repo && cd feature_repo

# Apply feature definitions to the registry
feast apply

# Materialize features to the online store
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# View registered features
feast feature-views list
feast entities list
```

## See Also

- [Feature Stores Concept](../concepts/feature-stores.md)
- [Training Data Pipelines](training-data-pipelines.md)
- [RAG Pipeline Implementation](rag-pipeline-implementation.md)
