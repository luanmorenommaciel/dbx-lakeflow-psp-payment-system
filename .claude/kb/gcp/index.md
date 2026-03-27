# GCP Serverless Data Engineering Knowledge Base

> **Purpose**: Google Cloud Platform serverless services for event-driven data pipelines
> **MCP Validated**: 2026-02-17
> **Last Updated**: 2026-03-26 (BigLake Iceberg, Cloud Run GPUs, Vertex AI Gemini 3, Dataflow ML)

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/cloud-run.md](concepts/cloud-run.md) | Cloud Run containers, scaling, environment variables |
| [concepts/pubsub.md](concepts/pubsub.md) | Pub/Sub messaging, topics, subscriptions |
| [concepts/gcs.md](concepts/gcs.md) | Cloud Storage buckets, lifecycle, event triggers |
| [concepts/bigquery.md](concepts/bigquery.md) | BigQuery tables, data loading, querying |
| [concepts/iam.md](concepts/iam.md) | IAM roles, service accounts, policies |
| [concepts/secret-manager.md](concepts/secret-manager.md) | Secret Manager versioning, access patterns |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/event-driven-pipeline.md](patterns/event-driven-pipeline.md) | GCS -> Pub/Sub -> Cloud Run pipeline |
| [patterns/multi-bucket-pipeline.md](patterns/multi-bucket-pipeline.md) | Multi-stage bucket workflow |
| [patterns/pubsub-fanout.md](patterns/pubsub-fanout.md) | Fan-out message distribution pattern |
| [patterns/cloud-run-scaling.md](patterns/cloud-run-scaling.md) | Auto-scaling and concurrency tuning |
| [patterns/gcs-triggered-workflow.md](patterns/gcs-triggered-workflow.md) | GCS event notification pattern |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/gcp-services.yaml](specs/gcp-services.yaml) | Service configuration reference |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables for all GCP services

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Cloud Run** | Fully managed serverless container platform with auto-scaling |
| **Pub/Sub** | Asynchronous messaging service for event-driven architectures |
| **GCS** | Object storage with event notifications for pipeline triggers |
| **BigQuery** | Serverless data warehouse for analytics at scale |
| **IAM** | Identity and access management with least-privilege service accounts |
| **Secret Manager** | Secure storage and versioned access for API keys and credentials |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/cloud-run.md, concepts/gcs.md, concepts/pubsub.md |
| **Intermediate** | patterns/event-driven-pipeline.md, patterns/gcs-triggered-workflow.md |
| **Advanced** | patterns/pubsub-fanout.md, patterns/multi-bucket-pipeline.md |

---

## GCP Data Platform Updates (2025-2026)

| Service | Key Update | Date |
|---------|-----------|------|
| **BigQuery** | BigLake Iceberg tables GA, BigLake Metastore GA, Iceberg REST Catalog API (Preview) | 2025 |
| **BigQuery** | Data Engineering Agent (Preview) - natural language pipeline creation | Nov 2025 |
| **BigQuery** | DataFrames 2.0 - multimodal data science with Pandas API | Apr 2025 |
| **Cloud Run** | GPU support GA (NVIDIA), pay-per-second, scale-to-zero, <5s startup | Jun 2025 |
| **Cloud Run** | IAP direct integration GA, multi-region HA (Preview), MCP server (Preview) | 2026 |
| **Dataflow** | H100 GPU support (A3 VMs), TPU support for ML workloads | Jan 2026 |
| **Vertex AI** | Gemini 3 GA, Gemini 2.5 Flash/Pro, Live API GA, Model Garden 200+ models | 2025 |
| **Vertex AI** | Agent Builder with tool governance, Agent Development Kit (ADK) | Dec 2025 |

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| ai-data-engineer-gcp | All concepts + patterns | Building serverless data pipelines |
| ai-prompt-specialist-gcp | concepts/cloud-run.md, concepts/iam.md | Deploying LLM services on GCP |
