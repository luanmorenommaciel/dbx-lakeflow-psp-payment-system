# AWS Lambda Knowledge Base

> **Purpose**: AWS Lambda serverless functions with Python handlers, SAM deployment, S3 triggers, and data processing
> **MCP Validated**: 2026-02-17
> **Last Updated**: 2026-03-26 (Python 3.14, Powertools v2.43+, managed instances)

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/lambda-handler.md](concepts/lambda-handler.md) | Python handler function patterns and best practices |
| [concepts/sam-templates.md](concepts/sam-templates.md) | SAM template structure and deployment |
| [concepts/s3-triggers.md](concepts/s3-triggers.md) | S3 event notifications and trigger configuration |
| [concepts/iam-policies.md](concepts/iam-policies.md) | Least-privilege IAM policies for Lambda |
| [concepts/layers.md](concepts/layers.md) | Lambda layers for dependencies (pandas, pyarrow) |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/file-processing.md](patterns/file-processing.md) | S3 file processing pipeline end-to-end |
| [patterns/error-handling.md](patterns/error-handling.md) | DLQ, retries, and structured error handling |
| [patterns/powertools-logging.md](patterns/powertools-logging.md) | AWS Powertools structured JSON logging |
| [patterns/parquet-output.md](patterns/parquet-output.md) | Write Parquet files to S3 with pyarrow |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/sam-template.yaml](specs/sam-template.yaml) | Complete SAM template spec for S3-triggered Lambda |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Lambda Handler** | Entry point function receiving event and context objects |
| **SAM Templates** | Infrastructure-as-code for serverless deployment |
| **S3 Triggers** | Event-driven invocation from S3 object operations |
| **IAM Policies** | Least-privilege execution roles for Lambda |
| **Layers** | Shared dependency packages (pandas, pyarrow, Powertools) |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/lambda-handler.md, concepts/sam-templates.md |
| **Intermediate** | concepts/s3-triggers.md, patterns/file-processing.md |
| **Advanced** | patterns/parquet-output.md, patterns/powertools-logging.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| lambda-builder | patterns/file-processing.md, patterns/parquet-output.md | Build S3-triggered data processing Lambdas |
| aws-lambda-architect | concepts/sam-templates.md, concepts/iam-policies.md | Design Lambda architecture and IAM policies |
