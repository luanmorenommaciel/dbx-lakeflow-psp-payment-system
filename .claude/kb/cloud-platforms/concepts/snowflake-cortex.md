# Snowflake Cortex & AI Platform

> **Purpose:** Snowflake AI capabilities -- Cortex AI functions, Snowpark, dynamic tables, Snowpipe, and Iceberg Tables
> **MCP Validated:** 2026-03-26

## Overview

Snowflake has evolved from a cloud data warehouse into an AI data platform. Cortex AI brings LLM-powered functions directly into SQL. Snowpark enables Python/Java/Scala DataFrames that execute natively on Snowflake compute. Dynamic tables replace complex task/stream pipelines with declarative transformations. Snowpipe and Snowpipe Streaming handle continuous ingestion. Iceberg Tables provide open-format interoperability.

## Key Concepts

### Cortex AI Functions (GA Nov 2025)

Built-in SQL functions that call hosted LLMs without infrastructure management. Seven functions reached GA in November 2025, delivering production-ready AI capabilities within the Snowflake SQL engine:

| Function | Purpose | Example Use Case | Status |
|----------|---------|------------------|--------|
| `AI_COMPLETE` | Text generation/completion (multimodal: text + images) | Summarization, Q&A, translation, image analysis | GA (Nov 2025) |
| `AI_CLASSIFY` | Classify text or images into categories | Sentiment buckets, topic tagging, image categorization | GA (Nov 2025) |
| `AI_EMBED` | Vector embeddings for text or images | Semantic search, RAG pipelines, clustering | GA (Nov 2025) |
| `AI_SIMILARITY` | Embedding similarity between two inputs | Deduplication, recommendation, matching | GA (Nov 2025) |
| `AI_TRANSCRIBE` | Audio/video transcription | Meeting notes, call center analytics | GA (Nov 2025) |
| `AI_TRANSLATE` | Language translation | Multi-language content | GA (earlier) |
| `AI_FILTER` | Boolean classification via LLM prompt | Row-level filtering with natural language | GA (earlier) |
| `AI_REDACT` | Automated PII protection | Compliance, data masking | 2025 |

Cortex supports models including **OpenAI GPT-5.2**, Mistral, Llama 3.1, and Arctic. Multimodal support handles text and images. No data leaves Snowflake's security perimeter.

**Key capability**: Cortex AI Functions now work in **incremental dynamic table refresh** (GA Sep 2025), enabling AI-powered enrichment that runs automatically as data updates.

### Cortex Agents (GA Nov 2025)

Orchestrate across structured and unstructured data sources:

- **Planning**: Parse requests, split tasks into subtasks, route across tools
- **Tool use**: Cortex Search (unstructured) + Cortex Analyst (structured)
- **Reflection**: Evaluate results, iterate, or generate final response
- **Monitoring**: Track metrics, analyze performance, refine behavior

### Snowflake Intelligence (Late 2025)

Unified natural language interface for business users to ask questions across governed data without writing SQL. Combines Cortex Analyst and Cortex Search.

### Snowpark

Server-side execution framework for Python, Java, and Scala:

- **DataFrames** push computation to Snowflake warehouse (no data egress)
- **UDFs/UDTFs** deploy custom Python functions as SQL-callable objects
- **Stored Procedures** run complex logic with full Snowpark session access
- **ML** via Snowpark ML for preprocessing, training, and model registry

### Dynamic Tables

Declarative pipeline layer replacing imperative task/stream chains:

- Define the transformation SQL; Snowflake manages refresh automatically
- `TARGET_LAG` controls freshness (seconds to hours)
- Dependency graph built automatically across dynamic table chains
- Replaces: staging task -> stream -> transform task -> target table

### Snowpipe & Snowpipe Streaming

| Feature | Snowpipe | Snowpipe Streaming |
|---------|----------|--------------------|
| Trigger | Event-driven (S3/GCS/Azure notification) | Client SDK push (Java/Python) |
| Latency | 1-2 minutes | Sub-second |
| Format | Files (Parquet, CSV, JSON) | Row-level inserts |
| Cost | Per-file serverless compute | Per-row serverless compute |

### Iceberg Tables

Snowflake-managed tables using Apache Iceberg format:

- Data stored in Parquet on customer's cloud storage
- Queryable from Spark, Trino, Flink without Snowflake compute
- Supports time travel, schema evolution, partition evolution
- Catalog interop via Snowflake Open Catalog (Polaris)

## When to Use

- **Cortex AI** -- You need LLM inference on warehouse data without MLOps overhead
- **Snowpark** -- Python/ML workloads that must stay within Snowflake governance
- **Dynamic Tables** -- Multi-step ELT pipelines where declarative refresh simplifies ops
- **Snowpipe Streaming** -- Real-time ingestion from applications or IoT devices
- **Iceberg Tables** -- Multi-engine analytics or avoiding vendor lock-in

## Trade-offs

### Cortex AI vs BQML vs Mosaic AI

| Dimension | Cortex AI | BQML | Mosaic AI |
|-----------|-----------|------|-----------|
| Interface | SQL functions (AI_*) | SQL CREATE MODEL + AI.GENERATE | Python SDK + UI |
| Model hosting | Managed (GPT-5.2, Mistral, Llama, Arctic) | Managed (Gemini 3.0, Claude, Llama) | Managed + custom fine-tune |
| Custom models | Cortex Fine-Tuning | Import TensorFlow/ONNX | Full MLflow lifecycle |
| Vector search | Cortex Search | Vector Search index | Vector Search + Reranker |
| Agents | Cortex Agents (GA) | Vertex AI Agents | Mosaic AI Agents |
| GPU access | Abstracted | Abstracted | Direct (GPU clusters) |
| Multimodal | Text + images (AI_COMPLETE) | Gemini 3.0 multimodal | Multi-modal serving |
| Strength | Simplicity, zero-MLOps, SQL-native | SQL-native ML + Gemini integration | Full ML/AI lifecycle |

### Platform Considerations

- Cortex functions consume credits per token; cost scales with prompt size
- Dynamic tables lack fine-grained scheduling (no cron); rely on `TARGET_LAG`
- Snowpark Python UDFs add cold-start latency on first invocation
- Iceberg Tables have write-throughput limits vs native Snowflake tables

## See Also

- [Snowflake Patterns](../patterns/snowflake-patterns.md) -- SQL implementation patterns
- [Cross-Platform Patterns](cross-platform-patterns.md) -- SQL dialect differences
- [Cost Optimization](../patterns/cost-optimization.md) -- Warehouse sizing and auto-suspend
