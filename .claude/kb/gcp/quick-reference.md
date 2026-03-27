# GCP Serverless Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated**: 2026-02-17
> **Last Updated**: 2026-03-26

## Core Services

| Service | Python Package | Primary Use |
|---------|---------------|-------------|
| Cloud Run | `google-cloud-run` | Container hosting, HTTP/event handlers, GPU workloads |
| Pub/Sub | `google-cloud-pubsub` | Async messaging, event routing |
| GCS | `google-cloud-storage` | Object storage, pipeline triggers |
| BigQuery | `google-cloud-bigquery` | Data warehouse, SQL analytics, Iceberg lakehouse |
| Dataflow | `apache-beam[gcp]` | Batch/streaming pipelines, ML inference |
| Vertex AI | `google-cloud-aiplatform` | Gemini 3, model garden, agent builder |
| IAM | `google-cloud-iam` | Access control, service accounts |
| Secret Manager | `google-cloud-secret-manager` | Credentials, API key storage |

## Common gcloud Commands

| Action | Command |
|--------|---------|
| Deploy Cloud Run | `gcloud run deploy SERVICE --image IMAGE --region REGION` |
| Create topic | `gcloud pubsub topics create TOPIC` |
| Create subscription | `gcloud pubsub subscriptions create SUB --topic TOPIC` |
| Create bucket | `gcloud storage buckets create gs://BUCKET` |
| Set GCS notification | `gcloud storage buckets notifications create gs://BUCKET --topic=TOPIC` |
| Query BigQuery | `bq query --use_legacy_sql=false 'SELECT ...'` |
| Access secret | `gcloud secrets versions access latest --secret=NAME` |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| HTTP API endpoint | Cloud Run (service) |
| Async event processing | Cloud Run + Pub/Sub trigger |
| File arrives -> process | GCS notification -> Pub/Sub -> Cloud Run |
| Scheduled batch job | Cloud Scheduler -> Pub/Sub -> Cloud Run |
| Stream analytics | Pub/Sub -> Dataflow -> BigQuery |
| Store pipeline results | BigQuery (structured) or GCS (files) |
| GPU inference / LLM serving | Cloud Run with GPU (NVIDIA, scale-to-zero) |
| Open lakehouse (Iceberg) | BigLake Iceberg tables + BigLake Metastore |
| ML pipeline / batch inference | Dataflow with GPU (H100) or TPU |
| AI agent development | Vertex AI Agent Builder + ADK |
| Natural language pipelines | BigQuery Data Engineering Agent (Preview) |

## Environment Variables (Cloud Run)

| Variable | Purpose |
|----------|---------|
| `PORT` | Cloud Run injects, default 8080 |
| `K_SERVICE` | Service name (auto-set) |
| `K_REVISION` | Revision name (auto-set) |
| `K_CONFIGURATION` | Configuration name (auto-set) |
| `GOOGLE_CLOUD_PROJECT` | Set manually for project ID |

## BigLake Iceberg Quick Reference

| Operation | Method |
|-----------|--------|
| Create Iceberg table | `CREATE TABLE ... WITH CONNECTION ... OPTIONS(file_format='PARQUET', table_format='ICEBERG')` |
| Metastore | BigLake Metastore (GA) - fully managed, serverless |
| Catalog API | Iceberg REST Catalog API (Preview) for cross-engine access |
| Storage | Customer-owned GCS buckets with auto-optimization |
| Features | Schema evolution, time travel, partitioning (Preview), multi-statement txns (Preview) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Use `latest` secret alias in prod | Pin to specific version numbers |
| Grant `roles/editor` to service accounts | Use least-privilege predefined roles |
| Hardcode credentials in containers | Use Workload Identity or Secret Manager |
| Use synchronous pulls for high throughput | Use StreamingPull or push subscriptions |
| Ignore message deduplication | Implement idempotent handlers |
| Use `SELECT *` in BigQuery | Query only needed columns |
| Modify BigLake Iceberg bucket files directly | Always write through BigQuery (prevents data loss) |
| Use Cloud Run CPU for ML inference | Use Cloud Run GPU (pay-per-second, scale-to-zero) |

## Related Documentation

| Topic | Path |
|-------|------|
| Getting Started | `concepts/cloud-run.md` |
| Full Index | `index.md` |
| Pipeline Patterns | `patterns/event-driven-pipeline.md` |
