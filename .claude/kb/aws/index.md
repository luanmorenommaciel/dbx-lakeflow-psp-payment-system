# AWS Knowledge Base

> **Purpose**: AWS serverless and data services for processing, analytics, and deployment
> **Sub-Domains**: Lambda, Deployment
> **Last Updated**: 2026-03-26

## Sub-Domains

| Domain | Path | Purpose | Agents |
|--------|------|---------|--------|
| [Lambda](lambda/index.md) | `aws/lambda/` | Serverless functions, SAM templates, S3 triggers, IAM policies | lambda-builder, aws-lambda-architect |
| [Deployment](deployment/index.md) | `aws/deployment/` | SAM CLI, AWS CLI, deploy workflows, local testing | aws-deployer |

## AWS Data Services Overview (2025-2026)

| Service | Latest Version | Key Update |
|---------|---------------|------------|
| **AWS Lambda** | Python 3.14 runtime | Managed instances for steady-state traffic, SnapStart for Python |
| **AWS Glue** | 5.0 GA | Spark 3.5.4, Python 3.11, Iceberg 1.7.1, Delta 3.3.0, Lake Formation FGAC |
| **Redshift Serverless** | Up to 1024 RPUs | MDDL (10x perf), queue-based QMR, Iceberg read/write, Serverless reservations |
| **MWAA** | Airflow 3.0 GA | Redesigned UI, event-driven scheduling, Task SDK, Python 3.12 |
| **SAM CLI** | Finch support | `sam sync --watch`, `sam build --watch` (proposed), package performance flags |

## Quick Navigation

### Lambda

| Type | Files |
|------|-------|
| Concepts | [lambda-handler](lambda/concepts/lambda-handler.md), [sam-templates](lambda/concepts/sam-templates.md), [s3-triggers](lambda/concepts/s3-triggers.md), [iam-policies](lambda/concepts/iam-policies.md), [layers](lambda/concepts/layers.md) |
| Patterns | [file-processing](lambda/patterns/file-processing.md), [error-handling](lambda/patterns/error-handling.md), [powertools-logging](lambda/patterns/powertools-logging.md), [parquet-output](lambda/patterns/parquet-output.md) |
| Specs | [sam-template.yaml](lambda/specs/sam-template.yaml) |

### Deployment

| Type | Files |
|------|-------|
| Concepts | [sam-cli](deployment/concepts/sam-cli.md), [aws-cli](deployment/concepts/aws-cli.md), [environments](deployment/concepts/environments.md) |
| Patterns | [sam-deploy](deployment/patterns/sam-deploy.md), [local-testing](deployment/patterns/local-testing.md), [s3-operations](deployment/patterns/s3-operations.md) |
| Specs | [deployment-config.yaml](deployment/specs/deployment-config.yaml) |
