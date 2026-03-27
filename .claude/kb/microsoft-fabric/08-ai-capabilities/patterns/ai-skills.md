> **MCP Validated:** 2026-02-17

# AI Skills Integration

> **Purpose**: Building and deploying AI Skills in Fabric for natural language data access and LLM-powered transforms

## When to Use

- Enabling business users to query data using natural language instead of SQL/KQL
- Applying LLM-powered transformations (summarize, classify, extract) to OneLake data
- Creating reusable AI endpoints that combine data context with language models
- Building chat-based analytics interfaces over Fabric datasets

## Implementation

```python
# Pattern 1: AI Functions in Spark notebooks for LLM transforms
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
import requests
import json

# AI function: classify customer feedback
def classify_sentiment(text: str) -> str:
    """Use Azure OpenAI to classify text sentiment."""
    if not text:
        return "unknown"
    endpoint = "https://your-aoai.openai.azure.com/openai/deployments/gpt-4o/chat/completions?api-version=2024-06-01"
    headers = {
        "Content-Type": "application/json",
        "api-key": "your-api-key"  # Use Fabric Key Vault in production
    }
    payload = {
        "messages": [
            {"role": "system", "content": "Classify sentiment as positive, negative, or neutral. Return only the label."},
            {"role": "user", "content": text}
        ],
        "max_tokens": 10,
        "temperature": 0
    }
    resp = requests.post(endpoint, headers=headers, json=payload)
    return resp.json()["choices"][0]["message"]["content"].strip().lower()

classify_udf = udf(classify_sentiment, StringType())

# Apply AI function to lakehouse data
df_feedback = spark.sql("SELECT * FROM silver_lakehouse.customer_feedback")
df_enriched = df_feedback.withColumn(
    "sentiment", classify_udf(col("feedback_text"))
)
df_enriched.write.format("delta").mode("overwrite").saveAsTable("gold_feedback_sentiment")


# Pattern 2: AI Skills via REST API for natural language queries
def create_ai_skill(workspace_id: str, headers: dict) -> dict:
    """Create an AI Skill that can answer questions about data."""
    payload = {
        "displayName": "SalesAnalyticsBot",
        "description": "Ask questions about sales data in natural language",
    }
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/aiSkills",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()


# Pattern 3: Batch scoring with PREDICT() in T-SQL
# After registering an MLflow model, use PREDICT in warehouse queries
"""
SELECT
    customer_id,
    total_purchases,
    avg_order_value,
    days_since_last_order,
    PREDICT(
        MODEL 'churn_prediction_model',
        DATA(total_purchases, avg_order_value, days_since_last_order)
    ) AS churn_probability
FROM dbo.dim_customer
WHERE is_active = 1;
"""
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| AI Skill type | Chat | Natural language Q&A over data |
| Model backend | Azure OpenAI | GPT-4o, GPT-4o-mini |
| Data sources | OneLake tables | Lakehouse, Warehouse tables |
| PREDICT function | T-SQL | MLflow registered models |
| AI Functions | Spark | UDFs with LLM calls |
| Rate limits | Per Azure OpenAI tier | TPM/RPM limits apply |

## Example Usage

```python
# Batch enrichment pipeline: classify + summarize + extract
from pyspark.sql.functions import col

# Read unstructured data
df = spark.sql("SELECT id, raw_text FROM bronze_documents")

# Chain AI transforms
df_classified = df.withColumn("category", classify_udf(col("raw_text")))

# Write enriched results to gold layer
df_classified.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_classified_documents")

# Verify results
display(spark.sql("""
    SELECT category, COUNT(*) as doc_count
    FROM gold_classified_documents
    GROUP BY category
    ORDER BY doc_count DESC
"""))
```

## AI Capability Matrix

| Capability | Engine | Use Case |
|------------|--------|----------|
| AI Skills | Azure OpenAI | Natural language Q&A |
| AI Functions | Spark + LLM | Batch text transforms |
| PREDICT() | MLflow + T-SQL | ML model inference |
| Copilot | Built-in | Code/query generation |
| SynapseML | Spark | Pre-built ML transforms |
| Semantic Link | Power BI + Notebook | BI-to-code bridge |

## See Also

- [Copilot and ML](../concepts/copilot-ml.md)
- [Lakehouse](../../02-data-engineering/concepts/lakehouse.md)
- [SDK Automation](../../05-apis-sdks/patterns/sdk-automation.md)
