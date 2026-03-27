> **MCP Validated:** 2026-02-17

# ML Model Lifecycle

> **Purpose**: End-to-end ML model lifecycle in Fabric -- MLflow experiment tracking, model registry, versioning, and PREDICT function
> **Confidence**: 0.95

## Overview

Microsoft Fabric provides a fully managed ML model lifecycle built on MLflow. Data scientists use Fabric notebooks to train models, track experiments with MLflow (auto-configured per workspace), register models in the Fabric model registry, version them, and serve predictions via the T-SQL `PREDICT` function or Spark batch scoring. The lifecycle spans experimentation, registration, staging, and production inference -- all within the Fabric platform without external ML infrastructure.

## Lifecycle Stages

```text
┌───────────┐    ┌───────────┐    ┌───────────┐    ┌───────────┐
│ EXPERIMENT│───▶│ REGISTER  │───▶│  STAGE    │───▶│  SERVE    │
│           │    │           │    │           │    │           │
│ - Train   │    │ - Version │    │ - Staging │    │ - PREDICT │
│ - Track   │    │ - Tag     │    │ - Prod    │    │ - Batch   │
│ - Compare │    │ - Describe│    │ - Archive │    │ - Spark   │
└───────────┘    └───────────┘    └───────────┘    └───────────┘
     MLflow           MLflow           MLflow         T-SQL/Spark
```

## Experiment Tracking

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, precision_score, recall_score

# MLflow is auto-configured in Fabric notebooks -- set custom experiment (optional)
mlflow.set_experiment("invoice_classification")

df = spark.sql("SELECT * FROM gold_lakehouse.training_features").toPandas()
X = df.drop(columns=["label"])
y = df["label"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train with full MLflow tracking
with mlflow.start_run(run_name="gbm_v2") as run:
    params = {"n_estimators": 200, "max_depth": 5, "learning_rate": 0.1}
    model = GradientBoostingClassifier(**params)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    metrics = {
        "accuracy": accuracy_score(y_test, preds),
        "precision": precision_score(y_test, preds, average="weighted"),
        "recall": recall_score(y_test, preds, average="weighted"),
    }

    mlflow.log_params(params)
    mlflow.log_metrics(metrics)
    mlflow.sklearn.log_model(model, "model")

    print(f"Run ID: {run.info.run_id}")
    print(f"Metrics: {metrics}")
```

## Model Registry

```python
# Register the best model
model_uri = f"runs:/{run.info.run_id}/model"
registered = mlflow.register_model(model_uri, "invoice_classifier")
print(f"Version: {registered.version}")

# Transition model stages
from mlflow.tracking import MlflowClient

client = MlflowClient()

# Promote to staging, then production
client.transition_model_version_stage(
    name="invoice_classifier", version=registered.version, stage="Staging",
)
client.transition_model_version_stage(
    name="invoice_classifier", version=registered.version, stage="Production",
)

# Add description and tags
client.update_model_version(
    name="invoice_classifier", version=registered.version,
    description="GBM classifier trained on 50k samples, accuracy 94.2%",
)
client.set_model_version_tag(
    name="invoice_classifier", version=registered.version,
    key="validated_by", value="data-science-team",
)
```

## PREDICT Function (T-SQL)

```sql
-- Use registered MLflow model for inference in Fabric Warehouse
SELECT
    invoice_id,
    vendor_name,
    total_amount,
    PREDICT(
        MODEL 'invoice_classifier',
        DATA(vendor_name_encoded, total_amount, line_item_count, has_tax)
    ) AS predicted_category
FROM dbo.staging_invoices
WHERE processing_status = 'pending';
```

## Quick Reference

| Operation | API/Method | Notes |
|-----------|-----------|-------|
| Create experiment | `mlflow.set_experiment()` | Auto-created per notebook |
| Log run | `mlflow.start_run()` | Params, metrics, artifacts |
| Log model | `mlflow.sklearn.log_model()` | Also: `pyspark`, `lightgbm` |
| Register | `mlflow.register_model()` | Creates versioned entry |
| Stage transition | `client.transition_model_version_stage()` | Staging, Production, Archived |
| Batch predict | `PREDICT()` T-SQL | Requires registered model |
| Spark predict | `mlflow.pyfunc.spark_udf()` | For large-scale scoring |

## Common Mistakes

### Wrong

```python
# Saving model as a file without registry
import pickle
with open("/lakehouse/default/model.pkl", "wb") as f:
    pickle.dump(model, f)
```

### Correct

```python
# Always use MLflow for model persistence in Fabric
mlflow.sklearn.log_model(model, "model")
mlflow.register_model(model_uri, "production_model")
```

## Related

- [Copilot and ML](copilot-ml.md)
- [Copilot Customization](copilot-customization.md)
- [Prediction Pipeline](../patterns/prediction-pipeline.md)
- [AI Skills](../patterns/ai-skills.md)
