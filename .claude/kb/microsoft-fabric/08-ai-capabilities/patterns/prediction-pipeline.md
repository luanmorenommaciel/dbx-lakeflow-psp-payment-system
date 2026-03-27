> **MCP Validated:** 2026-02-17

# End-to-End ML Prediction Pipeline

> **Purpose**: Complete pattern for data prep in Lakehouse, model training with MLflow, and batch scoring with PREDICT in Fabric

## When to Use

- Building a repeatable ML pipeline entirely within Fabric
- Training models on Lakehouse data and scoring in Warehouse
- Automating batch predictions on a schedule

## Overview

This pattern covers the full ML prediction lifecycle in Fabric: preparing features in a Lakehouse notebook, training a model with MLflow, registering it, and running batch predictions using the T-SQL `PREDICT` function or Spark batch scoring. Orchestrated via Data Factory pipelines.

## Architecture

```text
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   LAKEHOUSE  │───▶│   NOTEBOOK   │───▶│   MODEL      │───▶│   PREDICT    │
│              │    │              │    │   REGISTRY   │    │              │
│ bronze/      │    │ Feature eng  │    │              │    │ T-SQL or     │
│ silver/      │    │ Train model  │    │ Version 1..N │    │ Spark batch  │
│ gold/        │    │ MLflow track │    │ Stage: Prod  │    │              │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
       │                    │                  │                    │
       └────────────────────┴──────────────────┴────────────────────┘
                        Orchestrated by Data Factory Pipeline
```

## Step 1: Feature Engineering (Lakehouse Notebook)

```python
from pyspark.sql.functions import col, datediff, current_date, avg, count, sum as _sum

# Read from silver layer
df_orders = spark.sql("SELECT * FROM silver_lakehouse.orders")
df_customers = spark.sql("SELECT * FROM silver_lakehouse.customers")

# Build feature table
df_features = (
    df_orders
    .groupBy("customer_id")
    .agg(
        count("order_id").alias("total_orders"),
        _sum("order_total").alias("lifetime_value"),
        avg("order_total").alias("avg_order_value"),
    )
    .join(df_customers, "customer_id")
    .withColumn(
        "days_since_last_order",
        datediff(current_date(), col("last_order_date")),
    )
    .select(
        "customer_id",
        "total_orders",
        "lifetime_value",
        "avg_order_value",
        "days_since_last_order",
        "churn_label",  # Target variable
    )
)

# Write feature table to gold layer
df_features.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_lakehouse.churn_features")

print(f"Feature table: {df_features.count()} rows")
```

## Step 2: Model Training with MLflow

```python
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import numpy as np

mlflow.set_experiment("churn_prediction")

# Load features
df = spark.sql("SELECT * FROM gold_lakehouse.churn_features").toPandas()
feature_cols = ["total_orders", "lifetime_value", "avg_order_value", "days_since_last_order"]
X = df[feature_cols]
y = df["churn_label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Hyperparameter search with tracking
param_grid = [
    {"n_estimators": 100, "max_depth": 5, "learning_rate": 0.1},
    {"n_estimators": 200, "max_depth": 7, "learning_rate": 0.05},
    {"n_estimators": 300, "max_depth": 5, "learning_rate": 0.01},
]

best_run_id = None
best_auc = 0

for params in param_grid:
    with mlflow.start_run(run_name=f"gbm_d{params['max_depth']}_n{params['n_estimators']}"):
        model = GradientBoostingClassifier(**params, random_state=42)
        model.fit(X_train, y_train)

        preds = model.predict(X_test)
        probs = model.predict_proba(X_test)[:, 1]

        metrics = {
            "accuracy": accuracy_score(y_test, preds),
            "f1_score": f1_score(y_test, preds),
            "auc_roc": roc_auc_score(y_test, probs),
            "cv_mean": np.mean(cross_val_score(model, X, y, cv=5)),
        }

        mlflow.log_params(params)
        mlflow.log_metrics(metrics)
        mlflow.sklearn.log_model(model, "model", input_example=X_test.head(1))

        if metrics["auc_roc"] > best_auc:
            best_auc = metrics["auc_roc"]
            best_run_id = mlflow.active_run().info.run_id

        print(f"Params: {params} | AUC: {metrics['auc_roc']:.4f}")

# Register best model
model_uri = f"runs:/{best_run_id}/model"
registered = mlflow.register_model(model_uri, "churn_predictor")
print(f"Registered model v{registered.version} (AUC: {best_auc:.4f})")
```

## Step 3a: Batch Scoring with PREDICT (T-SQL)

```sql
-- Score active customers using the registered model
SELECT
    customer_id,
    total_orders,
    lifetime_value,
    avg_order_value,
    days_since_last_order,
    PREDICT(
        MODEL 'churn_predictor',
        DATA(total_orders, lifetime_value, avg_order_value, days_since_last_order)
    ) AS churn_prediction
INTO dbo.churn_scores
FROM dbo.active_customers;

-- Query high-risk customers
SELECT customer_id, churn_prediction, lifetime_value
FROM dbo.churn_scores
WHERE churn_prediction = 1
ORDER BY lifetime_value DESC;
```

## Step 3b: Batch Scoring with Spark

```python
import mlflow.pyfunc

# Load production model as Spark UDF
model_uri = "models:/churn_predictor/Production"
predict_udf = mlflow.pyfunc.spark_udf(spark, model_uri, result_type="int")

# Score all active customers
df_score = spark.sql("SELECT * FROM gold_lakehouse.churn_features WHERE churn_label IS NULL")

df_predictions = df_score.withColumn(
    "churn_prediction",
    predict_udf("total_orders", "lifetime_value", "avg_order_value", "days_since_last_order"),
)

# Write predictions
df_predictions.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_lakehouse.churn_predictions")

print(f"Scored {df_predictions.count()} customers")
```

## Orchestration (Data Factory)

| Activity | Type | Purpose |
|----------|------|---------|
| 1. Feature engineering | Notebook activity | Run feature prep notebook |
| 2. Model training | Notebook activity | Train and register model |
| 3. Batch scoring | Notebook or SP activity | Run PREDICT or Spark scoring |
| 4. Notification | Web activity | Alert on completion/failure |

## See Also

- [ML Model Lifecycle](../concepts/ml-model-lifecycle.md)
- [Copilot and ML](../concepts/copilot-ml.md)
- [AI Skills](ai-skills.md)
- [Lakehouse](../../02-data-engineering/concepts/lakehouse.md)
