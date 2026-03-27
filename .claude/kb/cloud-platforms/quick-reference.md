# Cloud Platforms Quick Reference

> Fast lookup tables. For code examples, see linked files.

## Platform Comparison (2026)

| Feature | Snowflake | Databricks | BigQuery |
|---------|-----------|-----------|----------|
| Architecture | Shared storage, compute isolation | Lakehouse (Delta + Spark) | Serverless, slot-based |
| Storage format | Proprietary (Iceberg support) | Delta Lake (open) | Capacitor (proprietary) |
| Open table formats | Iceberg (read), Polaris catalog | Delta + Iceberg (Unity) | BigLake (external) |
| SQL dialect | ANSI + extensions | Spark SQL + ANSI | GoogleSQL |
| Python support | Snowpark | PySpark / notebooks | BigFrames |
| AI/ML built-in | Cortex AI | Mosaic AI, MLflow | BQML, AI.GENERATE |
| Streaming | Snowpipe Streaming | Structured Streaming | BigQuery sub |
| Serverless | Yes (always) | Yes (serverless compute) | Yes (always) |
| Cost model | Credit-based (per-second) | DBU-based | Slot-based or on-demand |

## AI Features Comparison (Updated Late 2025)

| Capability | Snowflake Cortex | Databricks | BigQuery |
|-----------|-----------------|-----------|----------|
| Text-to-SQL | Cortex Analyst (GA) | Natural language queries | Gemini in BigQuery |
| Embeddings | AI_EMBED (GA Nov 2025) | Mosaic AI | AI.EMBED (GA Jan 2026) |
| Classification | AI_CLASSIFY (GA Nov 2025) | Custom models | AI.GENERATE |
| Summarization | AI_COMPLETE (GA Nov 2025) | Foundation models | AI.GENERATE |
| Similarity | AI_SIMILARITY (GA Nov 2025) | Vector Search | AI.SIMILARITY (GA Jan 2026) |
| Transcription | AI_TRANSCRIBE (GA Nov 2025) | Whisper via serving | Speech-to-Text API |
| PII Redaction | AI_REDACT (2025) | Unity Catalog tags | DLP API |
| ML Training | Snowpark ML | MLflow + AutoML | BQML CREATE MODEL |
| LLM serving | Cortex (GPT-5.2, Claude, Mistral, Llama) | Model Serving (GPT-5.2, Claude 4.5, Gemini 3) | Vertex AI (Gemini 3.0) |
| Agents | Cortex Agents (GA Nov 2025) | Mosaic AI Agents | Vertex AI Agents |
| Multimodal | Text + images (AI_COMPLETE) | Multi-modal serving | Gemini 3.0 multimodal |

## Pricing Models

| Platform | Compute | Storage | Key Cost Driver |
|----------|---------|---------|-----------------|
| Snowflake | Credits ($2-4/credit) | $23/TB/month | Warehouse size + runtime |
| Databricks | DBUs ($0.07-0.55/DBU) | Cloud storage cost | Cluster size + runtime |
| BigQuery | $6.25/TB scanned (on-demand) | $0.02/GB/month | Query volume or slots |

## What's New (Late 2025 - Early 2026)

| Platform | Feature | Status |
|----------|---------|--------|
| Snowflake | 7 Cortex AI Functions GA | GA (Nov 2025) |
| Snowflake | Cortex Agents (structured + unstructured) | GA (Nov 2025) |
| Snowflake | AI_COMPLETE multimodal (text + images) | GA (Nov 2025) |
| Snowflake | Cortex AI in incremental dynamic tables | GA (Sep 2025) |
| Snowflake | Snowflake Intelligence | GA (Late 2025) |
| Databricks | LakeFlow Connect (Salesforce, Workday) | GA (Feb 2025) |
| Databricks | 20+ LakeFlow connectors (MySQL, PostgreSQL, NetSuite, Meta Ads, etc.) | GA (Dec 2025) |
| Databricks | Lakebase with autoscaling, scale-to-zero | GA (Dec 2025) |
| Databricks | Vector Search Reranker | GA (Dec 2025) |
| BigQuery | AI.GENERATE, AI.EMBED, AI.SIMILARITY | GA (Jan 2026) |
| BigQuery | Gemini 3.0 Pro/Flash support | GA (Jan 2026) |
| BigQuery | End User Credentials (simplified AI setup) | GA (Jan 2026) |
| BigQuery | Multimodal Tables (structured + unstructured) | Preview (Apr 2025) |
| BigQuery | Apache Iceberg support | Preview (Apr 2025) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Leave warehouses running | Auto-suspend after 1-5 min |
| Scan full tables (BigQuery) | Partition + cluster, use LIMIT in dev |
| Over-provision Databricks clusters | Start small, use autoscaling |
| Ignore cross-region costs | Co-locate storage and compute |
| Vendor lock-in on proprietary features | Use Iceberg/Delta for portability |
| Build custom ingestion from SaaS | Use LakeFlow Connect (Databricks) or Snowpipe |
| Set up separate AI infrastructure | Use built-in Cortex AI / AI.GENERATE / Mosaic AI |
