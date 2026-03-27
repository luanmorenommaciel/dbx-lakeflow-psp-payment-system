# AWS Deployment Knowledge Base

> **Purpose**: AWS serverless deployment operations using SAM CLI, AWS CLI, S3, and Lambda
> **MCP Validated**: 2026-02-17

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/sam-cli.md](concepts/sam-cli.md) | SAM CLI commands: init, build, deploy, sync, validate |
| [concepts/aws-cli.md](concepts/aws-cli.md) | AWS CLI essentials: configure, profiles, output formats |
| [concepts/environments.md](concepts/environments.md) | Multi-environment deployment with samconfig.toml |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/sam-deploy.md](patterns/sam-deploy.md) | SAM build, package, deploy workflow end-to-end |
| [patterns/local-testing.md](patterns/local-testing.md) | sam local invoke, start-api, generate-event |
| [patterns/s3-operations.md](patterns/s3-operations.md) | S3 sync, cp, ls, presign patterns |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/deployment-config.yaml](specs/deployment-config.yaml) | Deployment configuration spec for samconfig.toml |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **SAM CLI** | Serverless Application Model CLI for building, testing, and deploying Lambda apps |
| **AWS CLI** | Unified command-line tool for managing AWS services and resources |
| **Environments** | Multi-environment strategy using samconfig.toml config-env sections |
| **S3 Operations** | Object storage commands for artifact management and static hosting |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/sam-cli.md, concepts/aws-cli.md |
| **Intermediate** | patterns/sam-deploy.md, patterns/local-testing.md |
| **Advanced** | concepts/environments.md, patterns/s3-operations.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| aws-deployer | patterns/sam-deploy.md, concepts/environments.md | Build, test, and deploy Lambda functions to AWS |
