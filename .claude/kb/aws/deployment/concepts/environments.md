# Environments

> **Purpose**: Multi-environment deployment strategy using samconfig.toml and parameter overrides
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

AWS SAM supports multi-environment deployments through `samconfig.toml` configuration
environments (config-env) and CloudFormation parameter overrides. Each environment (dev,
staging, prod) gets its own stack name, S3 bucket, and parameter values. The `--config-env`
flag selects which configuration section to use during build and deploy.

## The Pattern

```toml
# samconfig.toml - Multi-environment configuration
version = 0.1

[default.build.parameters]
use_container = true
parallel = true

[dev.deploy.parameters]
stack_name = "myapp-dev"
s3_bucket = "myapp-sam-artifacts-dev"
s3_prefix = "myapp"
region = "us-east-1"
capabilities = "CAPABILITY_IAM CAPABILITY_AUTO_EXPAND"
confirm_changeset = false
parameter_overrides = [
    "Environment=dev",
    "LogLevel=DEBUG",
    "MemorySize=256"
]

[staging.deploy.parameters]
stack_name = "myapp-staging"
s3_bucket = "myapp-sam-artifacts-staging"
s3_prefix = "myapp"
region = "us-east-1"
capabilities = "CAPABILITY_IAM CAPABILITY_AUTO_EXPAND"
confirm_changeset = true
parameter_overrides = [
    "Environment=staging",
    "LogLevel=INFO",
    "MemorySize=512"
]

[prod.deploy.parameters]
stack_name = "myapp-prod"
s3_bucket = "myapp-sam-artifacts-prod"
s3_prefix = "myapp"
region = "us-east-1"
capabilities = "CAPABILITY_IAM CAPABILITY_AUTO_EXPAND"
confirm_changeset = true
parameter_overrides = [
    "Environment=prod",
    "LogLevel=WARNING",
    "MemorySize=1024"
]
```

## Quick Reference

| Environment | Command | Changeset Confirm |
|-------------|---------|-------------------|
| dev | `sam deploy --config-env dev` | No (fast iteration) |
| staging | `sam deploy --config-env staging` | Yes (review changes) |
| prod | `sam deploy --config-env prod` | Yes (always review) |

## Template Parameters

```yaml
# template.yaml - Parameters section
Parameters:
  Environment:
    Type: String
    Default: dev
    AllowedValues: [dev, staging, prod]

  LogLevel:
    Type: String
    Default: INFO
    AllowedValues: [DEBUG, INFO, WARNING, ERROR]

  MemorySize:
    Type: Number
    Default: 256
    AllowedValues: [128, 256, 512, 1024, 2048]

Resources:
  MyFunction:
    Type: AWS::Serverless::Function
    Properties:
      FunctionName: !Sub "myapp-${Environment}-handler"
      MemorySize: !Ref MemorySize
      Environment:
        Variables:
          ENV: !Ref Environment
          LOG_LEVEL: !Ref LogLevel
```

## Common Mistakes

### Wrong

```bash
# Deploying to prod without specifying environment
sam deploy

# Using same stack name across environments
# samconfig.toml with stack_name = "myapp" for all envs
```

### Correct

```bash
# Always specify config-env
sam build && sam deploy --config-env dev

# Unique stack names per environment
# stack_name = "myapp-dev" / "myapp-staging" / "myapp-prod"
```

## Environment Promotion Flow

```text
dev (auto-deploy) -> staging (manual approval) -> prod (changeset review)
```

```bash
# Promote through environments
sam build
sam deploy --config-env dev
# ... run tests ...
sam deploy --config-env staging
# ... run integration tests ...
sam deploy --config-env prod
```

## Related

- [SAM CLI](../concepts/sam-cli.md)
- [SAM Deploy Pattern](../patterns/sam-deploy.md)
- [Deployment Config Spec](../specs/deployment-config.yaml)
