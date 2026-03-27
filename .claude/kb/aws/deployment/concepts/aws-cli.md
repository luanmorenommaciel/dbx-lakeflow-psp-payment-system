# AWS CLI

> **Purpose**: Unified command-line interface for managing AWS services and resources
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

AWS CLI v2 is the primary tool for interacting with AWS services from the terminal. It
supports named profiles for multi-account management, output formatting (json, table, text),
and auto-completion. For serverless deployments, it complements SAM CLI by providing direct
access to Lambda, S3, CloudFormation, IAM, and CloudWatch services.

## The Pattern

```bash
# Configure credentials (stored in ~/.aws/credentials)
aws configure --profile my-project-dev

# Verify identity
aws sts get-caller-identity --profile my-project-dev

# Set default profile for session
export AWS_PROFILE=my-project-dev
export AWS_DEFAULT_REGION=us-east-1

# Common operations
aws lambda list-functions --output table
aws cloudformation describe-stacks --stack-name my-stack
aws logs tail /aws/lambda/my-function --follow
```

## Quick Reference

| Command | Input | Output | Notes |
|---------|-------|--------|-------|
| `aws configure` | credentials | ~/.aws/credentials | Interactive setup |
| `aws configure list` | -- | Current config | Shows active profile |
| `aws sts get-caller-identity` | -- | Account/ARN/UserId | Verify who you are |
| `aws lambda list-functions` | -- | Function list | Add `--output table` |
| `aws lambda invoke` | function name | Response payload | Direct invocation |
| `aws cloudformation describe-stacks` | stack name | Stack details | Status, outputs |
| `aws logs tail` | log group | Log events | `--follow` for streaming |

## Profile Management

```bash
# ~/.aws/credentials
[default]
aws_access_key_id = AKIA...
aws_secret_access_key = wJal...

[my-project-dev]
aws_access_key_id = AKIA...
aws_secret_access_key = kBzp...

[my-project-prod]
role_arn = arn:aws:iam::123456789012:role/DeployRole
source_profile = default

# ~/.aws/config
[profile my-project-dev]
region = us-east-1
output = json

[profile my-project-prod]
region = us-east-1
output = json
```

## Common Mistakes

### Wrong

```bash
# Hardcoding credentials in scripts
aws s3 ls --access-key AKIA... --secret-key wJal...

# Using default profile in production
aws lambda invoke --function-name prod-function out.json
```

### Correct

```bash
# Use named profiles
aws s3 ls --profile my-project-prod

# Or export for session
export AWS_PROFILE=my-project-prod
aws lambda invoke --function-name prod-function out.json
```

## Output Formats

| Format | Flag | Use Case |
|--------|------|----------|
| JSON | `--output json` | Programmatic parsing, jq pipelines |
| Table | `--output table` | Human-readable display |
| Text | `--output text` | Shell scripting, awk/cut |
| YAML | `--output yaml` | Readable structured output |

## Useful Lambda Commands

```bash
# Get function configuration
aws lambda get-function-configuration --function-name MyFunc

# Update function code directly (without SAM)
aws lambda update-function-code \
  --function-name MyFunc \
  --zip-file fileb://function.zip

# Update environment variables
aws lambda update-function-configuration \
  --function-name MyFunc \
  --environment "Variables={ENV=prod,LOG_LEVEL=INFO}"

# Invoke and capture response
aws lambda invoke \
  --function-name MyFunc \
  --payload '{"key": "value"}' \
  --cli-binary-format raw-in-base64-out \
  response.json
```

## Environment Variables

| Variable | Purpose |
|----------|---------|
| `AWS_PROFILE` | Active named profile |
| `AWS_DEFAULT_REGION` | Default region |
| `AWS_ACCESS_KEY_ID` | Access key (overrides profile) |
| `AWS_SECRET_ACCESS_KEY` | Secret key (overrides profile) |
| `AWS_SESSION_TOKEN` | Temporary session token |
| `AWS_DEFAULT_OUTPUT` | Output format |

## Related

- [SAM CLI](../concepts/sam-cli.md)
- [Environments](../concepts/environments.md)
- [S3 Operations](../patterns/s3-operations.md)
