# BigQuery AI & ML Platform

> **Purpose:** BigQuery ML, AI.GENERATE with Gemini, BigLake external tables, and Dataform for transformation
> **Confidence:** 0.90
> **MCP Validated:** 2026-03-26

## Overview

BigQuery has evolved from a serverless data warehouse into an AI-native analytics platform. BQML brings ML training and inference into SQL. AI.GENERATE calls Gemini models directly from queries. BigLake unifies access to data on GCS, S3, and Azure Blob. Dataform provides dbt-like SQL transformation workflows natively in GCP.

## The Concept

### BigQuery ML (BQML)

Train, evaluate, and predict with ML models using SQL syntax. Supports linear regression, boosted trees, DNN, time series (ARIMA_PLUS), and imported TensorFlow/ONNX models.

```sql
-- Train a classification model with BQML
CREATE OR REPLACE MODEL `analytics.ml_models.churn_predictor`
OPTIONS(
  model_type = 'BOOSTED_TREE_CLASSIFIER',
  input_label_cols = ['churned'],
  data_split_method = 'AUTO_SPLIT',
  max_iterations = 50,
  learn_rate = 0.1,
  enable_global_explain = TRUE
) AS
SELECT
  customer_id,
  tenure_months,
  monthly_spend,
  support_tickets_last_90d,
  product_count,
  churned
FROM `analytics.features.customer_features`
WHERE snapshot_date = CURRENT_DATE() - 1;

-- Evaluate the model
SELECT *
FROM ML.EVALUATE(MODEL `analytics.ml_models.churn_predictor`);

-- Predict on new data
SELECT
  customer_id,
  predicted_churned,
  predicted_churned_probs
FROM ML.PREDICT(
  MODEL `analytics.ml_models.churn_predictor`,
  (SELECT * FROM `analytics.features.customer_features`
   WHERE snapshot_date = CURRENT_DATE())
);
```

### AI Functions (GA Jan 2026)

BigQuery now offers dedicated AI functions integrated with Gemini 3.0 Pro/Flash, with simplified End User Credentials (EUC) setup -- no separate connection or service account needed:

| Function | Purpose | Status |
|----------|---------|--------|
| `AI.GENERATE()` | Text and structured data generation via LLMs | GA (Jan 2026) |
| `AI.EMBED()` | Embedding generation for text and images | GA (Jan 2026) |
| `AI.SIMILARITY()` | Semantic similarity scores between text/images | GA (Jan 2026) |
| `ML.GENERATE_TEXT()` | Text generation via Gemini, Claude, Llama | GA |
| `ML.GENERATE_EMBEDDING()` | Text or multimodal embeddings | GA |
| `AI.GENERATE_TABLE()` | Structured tabular data from LLMs | GA |

```sql
-- Summarize customer feedback using AI.GENERATE (simplified setup)
SELECT
  feedback_id,
  feedback_text,
  AI.GENERATE(
    MODEL `analytics.ml_models.gemini_pro`,
    CONCAT(
      'Summarize this customer feedback in one sentence. ',
      'Extract sentiment (positive/negative/neutral). ',
      'Feedback: ', feedback_text
    )
  ).result AS ai_summary
FROM `analytics.raw.customer_feedback`
WHERE created_date >= CURRENT_DATE() - 7;

-- Compute semantic similarity between queries
SELECT
  AI.SIMILARITY(query_a, query_b) AS similarity_score
FROM search_pairs;
```

### BigLake

External tables that unify access to data across cloud storage providers:

| Feature | Description |
|---------|-------------|
| GCS Integration | Native external tables on Google Cloud Storage |
| S3 / Azure Blob | Cross-cloud external tables via BigLake connections |
| Fine-Grained Security | Row/column-level access on external data |
| Iceberg Support | Read Apache Iceberg tables as BigLake tables |
| Metastore | BigLake Metastore for open-format catalog |

### Dataform

SQL-based transformation framework native to GCP (acquired from open-source project):

- SQLX files with `config` blocks (similar to dbt `config()`)
- Dependency declarations via `ref()` function
- Assertions for data quality (like dbt tests)
- JavaScript for reusable macros and includes
- Git-integrated with scheduling via Cloud Composer or BigQuery

### Multimodal Tables (Preview Apr 2025)

Store and analyze structured and unstructured data (images, documents, audio) in a single BigQuery table. Enables AI queries across mixed data types.

### Apache Iceberg Support (Preview Apr 2025)

BigQuery can now read and write Apache Iceberg tables on GCS, enabling multi-engine analytics and reducing vendor lock-in.

## Quick Reference

| Capability | SQL Syntax | Use Case |
|------------|-----------|----------|
| Train model | `CREATE MODEL` | ML from SQL |
| Predict | `ML.PREDICT()` | Batch scoring |
| Evaluate | `ML.EVALUATE()` | Model metrics |
| Generate text | `AI.GENERATE()` | LLM from SQL (Gemini 3.0) |
| Embed text/images | `AI.EMBED()` | Vector creation |
| Similarity | `AI.SIMILARITY()` | Semantic matching |
| Structured generation | `AI.GENERATE_TABLE()` | Tabular output from LLMs |
| External table | `CREATE EXTERNAL TABLE` | Cross-cloud access |

## Common Mistakes

### Wrong
```sql
-- Running AI.GENERATE without any model setup
SELECT AI.GENERATE('summarize this text');
```

### Correct (New: End User Credentials -- simplified setup)
```sql
-- With EUC (Jan 2026+), no separate connection needed for Vertex AI models
-- Just authenticate via your user credentials
SELECT AI.GENERATE(
  MODEL `project.dataset.gemini_flash`,
  'Summarize this text'
).result;
```

### Correct (Legacy: Connection-based setup)
```sql
-- Create a Cloud Resource connection and remote model
CREATE OR REPLACE MODEL `project.dataset.gemini_pro`
  REMOTE WITH CONNECTION `project.region.connection_id`
  OPTIONS(ENDPOINT = 'gemini-pro');

-- Then use it in AI.GENERATE
SELECT AI.GENERATE(MODEL `project.dataset.gemini_pro`, prompt).result;
```

## Related

- [BigQuery Patterns](../patterns/bigquery-patterns.md) -- Scheduled queries, BQML workflows, remote functions
- [Cross-Platform Patterns](cross-platform-patterns.md) -- BQML vs Cortex AI vs Mosaic AI
- [Cost Optimization](../patterns/cost-optimization.md) -- Slot reservations vs on-demand pricing
