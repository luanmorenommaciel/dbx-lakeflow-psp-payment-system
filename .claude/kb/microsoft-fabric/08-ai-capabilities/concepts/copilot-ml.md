> **MCP Validated:** 2026-03-26

# Copilot and ML Models

> **Purpose**: AI-powered assistance and machine learning capabilities in Microsoft Fabric
> **Confidence**: 0.95

## Overview

Microsoft Fabric Copilot is powered by Azure OpenAI and provides AI assistance across all Fabric workloads -- Data Factory, Data Engineering, Data Science, Warehouse, and Power BI. It reads workspace metadata, schemas, and context to generate code, queries, and visualizations. Fabric also includes a full Data Science workload with MLflow integration for training, tracking, and deploying ML models directly within the platform.

**Key 2025 updates:**
- **Copilot available on F2+ SKUs** (April 2025) -- no longer requires F64 or higher
- **Fabric Copilot Capacity (FCC)** -- consolidated billing for Copilot usage across capacities
- **AI Functions in Warehouse** (preview) -- T-SQL functions for sentiment analysis, text classification, entity extraction, and summarization powered by LLMs
- **Fabric Data Agents** -- AI agents that enhance data-driven decision-making with natural language interfaces
- **Copilot sidecar chat tools** (Nov 2025) -- real-time data exploration with conversational AI

## The Pattern

```python
# Fabric Data Science notebook -- MLflow model training
import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score

# Read data from Lakehouse
df = spark.sql("SELECT * FROM gold_customers").toPandas()

# Prepare features
X = df[["total_purchases", "avg_order_value", "days_since_last_order"]]
y = df["churn_label"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

# Train with MLflow tracking (auto-configured in Fabric)
with mlflow.start_run(run_name="churn_rf_v1"):
    model = RandomForestClassifier(n_estimators=100, max_depth=10)
    model.fit(X_train, y_train)

    predictions = model.predict(X_test)
    accuracy = accuracy_score(y_test, predictions)
    f1 = f1_score(y_test, predictions)

    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    mlflow.log_metric("accuracy", accuracy)
    mlflow.log_metric("f1_score", f1)
    mlflow.sklearn.log_model(model, "churn_model")

    print(f"Accuracy: {accuracy:.4f}, F1: {f1:.4f}")

# Register model for deployment
model_uri = f"runs:/{mlflow.active_run().info.run_id}/churn_model"
mlflow.register_model(model_uri, "churn_prediction_model")
```

## Quick Reference

| Copilot Feature | Workload | Capability |
|-----------------|----------|------------|
| Code generation | Notebooks | Generate PySpark/Python code from prompts |
| Query generation | Warehouse | Write T-SQL from natural language |
| DAX assistance | Power BI | Generate DAX measures and calculations |
| Pipeline design | Data Factory | Suggest pipeline activities and structure |
| Data profiling | Data Science | Summarize datasets, suggest transformations |
| Report creation | Power BI | Build visuals from natural language |
| Sidecar chat | All workloads | Real-time conversational data exploration (Nov 2025) |
| Data agents | Platform | Natural language interfaces for data-driven decisions |

## AI Functions in Warehouse (Preview)

T-SQL functions powered by LLMs, available directly in Warehouse queries:

```sql
-- Classify text using AI Functions
SELECT
    feedback_id,
    feedback_text,
    AI.CLASSIFY(feedback_text, 'positive', 'negative', 'neutral') AS sentiment
FROM customer_feedback;

-- Extract structured data from unstructured text
SELECT
    ticket_id,
    AI.EXTRACT(description, 'product_name, issue_type, severity') AS extracted
FROM support_tickets;
```

## ML Capabilities

| Feature | Description |
|---------|-------------|
| MLflow tracking | Auto-configured experiment tracking |
| Model registry | Centralized model versioning |
| PREDICT function | T-SQL function for model inference |
| AI Functions | LLM-powered transforms in T-SQL (sentiment, classify, extract) |
| Batch scoring | Spark-based batch predictions |
| SynapseML | Pre-built ML transforms for Spark |
| Fabric Data Agents | Natural language AI agents for data exploration |

## Common Mistakes

### Wrong

```python
# Deploying models outside of Fabric MLflow registry
import pickle
with open("model.pkl", "wb") as f:
    pickle.dump(model, f)
```

### Correct

```python
# Use MLflow for consistent model management in Fabric
mlflow.sklearn.log_model(model, "model_artifact")
mlflow.register_model(model_uri, "production_model")
# Then use PREDICT() in T-SQL for inference
```

## Related

- [AI Skills](../patterns/ai-skills.md)
- [Lakehouse](../../02-data-engineering/concepts/lakehouse.md)
- [REST API](../../05-apis-sdks/concepts/rest-api.md)
