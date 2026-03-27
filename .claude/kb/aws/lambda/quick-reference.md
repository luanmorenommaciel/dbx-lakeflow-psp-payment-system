# AWS Lambda Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-02-17
> **Last Updated:** 2026-03-26

## Runtime Limits

| Resource | Limit | Notes |
|----------|-------|-------|
| Memory | 128 MB - 10,240 MB | Increase for pandas/pyarrow |
| Timeout | max 900s (15 min) | Default 3s, set explicitly |
| Package size (zipped) | 50 MB | Direct upload |
| Package size (unzipped) | 250 MB | Including layers |
| /tmp storage | 512 MB - 10,240 MB | Ephemeral, configurable |
| Env variables | 4 KB total | Use SSM for large configs |
| Concurrent executions | 1,000 default | Request increase via quota |

## Supported Python Runtimes (2026)

| Runtime | Status | EOL |
|---------|--------|-----|
| Python 3.14 | GA (Nov 2025) | LTS |
| Python 3.13 | GA | Active |
| Python 3.12 | GA | Active |
| Python 3.11 | GA | Active |
| Python 3.10 | Deprecated | Migrate to 3.12+ |
| Python 3.9 | Deprecated | Migrate to 3.12+ |

## New Features (re:Invent 2025+)

| Feature | Description |
|---------|-------------|
| **Managed Instances** | Steady-state compute for predictable traffic (no cold starts) |
| **SnapStart for Python** | Pre-initialized execution environments for faster cold starts |
| **Python 3.14 runtime** | Template strings (`t-strings`), deferred type annotation eval |
| **AWS Transform custom** | AI agent for automated runtime upgrades at scale |

## Handler Signature

| Component | Value | Example |
|-----------|-------|---------|
| Handler setting | `{file}.{function}` | `app.lambda_handler` |
| Event parameter | `dict` | S3 event payload |
| Context parameter | `LambdaContext` | Request ID, remaining time |
| Return | `dict`, `str`, `None` | JSON-serializable response |

## SAM CLI Commands

| Command | Purpose |
|---------|---------|
| `sam init` | Create new project from template |
| `sam build` | Build Lambda deployment artifacts |
| `sam local invoke` | Test function locally |
| `sam local start-api` | Local API Gateway emulation |
| `sam deploy --guided` | Interactive first deploy |
| `sam deploy` | Deploy with saved config |
| `sam logs -n FnName --tail` | Tail CloudWatch logs |
| `sam delete` | Remove deployed stack |

## S3 Event Types

| Event | When |
|-------|------|
| `s3:ObjectCreated:*` | Any object created |
| `s3:ObjectCreated:Put` | PUT upload only |
| `s3:ObjectCreated:Post` | POST upload only |
| `s3:ObjectCreated:CompleteMultipartUpload` | Multipart upload done |
| `s3:ObjectRemoved:*` | Any object deleted |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Process S3 uploads | S3 trigger + async invocation |
| REST API endpoint | API Gateway + sync invocation |
| Large dependencies (pandas) | Lambda Layer or container image |
| File > 500 MB processing | Step Functions + EFS mount |
| Retry failed events | DLQ (SQS) + EventInvokeConfig |
| Structured logging | Powertools Logger |
| Steady-state traffic (always-on) | Managed instances (re:Invent 2025) |
| Fast cold starts (Python) | SnapStart for Python |
| ETL with Spark | AWS Glue 5.0 (Spark 3.5.4, Python 3.11) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Initialize SDK clients inside handler | Initialize outside handler (reuse) |
| Use `*` in IAM resource ARNs | Scope to specific bucket/prefix |
| Set timeout to default 3s | Set timeout >= expected duration |
| Bundle pandas in zip (250 MB+) | Use AWS SDK for pandas managed layer |
| Catch bare `Exception` silently | Log errors, use Powertools, raise |
| Hardcode bucket names | Use environment variables |

## Related Documentation

| Topic | Path |
|-------|------|
| Handler patterns | `concepts/lambda-handler.md` |
| SAM templates | `concepts/sam-templates.md` |
| Full Index | `index.md` |
