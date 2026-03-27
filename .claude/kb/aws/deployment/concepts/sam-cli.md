# SAM CLI

> **Purpose**: AWS Serverless Application Model CLI for building, testing, and deploying Lambda applications
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

AWS SAM CLI is a command-line tool that extends AWS CloudFormation with a simplified syntax
for defining serverless resources (Lambda, API Gateway, DynamoDB, S3). It uses a
`template.yaml` file to declare infrastructure and provides local testing via Docker
containers that simulate the Lambda runtime environment.

## The Pattern

```bash
# Full lifecycle: init -> build -> test -> deploy
sam init --runtime python3.12 --app-template hello-world --name my-app
cd my-app

# Build artifacts (use --use-container for native dependencies)
sam build --use-container --parallel

# Test locally before deploying
sam local invoke HelloWorldFunction -e events/event.json

# Deploy with guided prompts (first time)
sam deploy --guided

# Deploy with saved config (subsequent times)
sam deploy --config-env dev
```

## Quick Reference

| Command | Input | Output | Notes |
|---------|-------|--------|-------|
| `sam init` | runtime, template | Project scaffold | Creates template.yaml |
| `sam build` | template.yaml | .aws-sam/ directory | Prepares deployment artifacts |
| `sam validate` | template.yaml | Validation result | Add `--lint` for cfn-lint |
| `sam deploy` | .aws-sam/ | CloudFormation stack | Uploads to S3 + deploys |
| `sam sync` | template.yaml | Live updates | `--watch` for hot reload |
| `sam delete` | stack name | Stack removed | Tears down all resources |
| `sam logs` | function name | CloudWatch output | `--tail` for streaming |
| `sam list` | -- | Stack resources | Shows endpoints, functions |

## template.yaml Structure

```yaml
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31
Description: SAM application

Globals:
  Function:
    Timeout: 30
    MemorySize: 256
    Runtime: python3.12
    Architectures:
      - x86_64

Parameters:
  Environment:
    Type: String
    Default: dev
    AllowedValues: [dev, staging, prod]

Resources:
  MyFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.lambda_handler
      CodeUri: src/
      Description: My Lambda function
      Environment:
        Variables:
          ENV: !Ref Environment
      Events:
        ApiEvent:
          Type: Api
          Properties:
            Path: /hello
            Method: get
      Policies:
        - S3ReadPolicy:
            BucketName: !Ref DataBucket

  DataBucket:
    Type: AWS::S3::Bucket

Outputs:
  ApiUrl:
    Description: API Gateway endpoint URL
    Value: !Sub "https://${ServerlessRestApi}.execute-api.${AWS::Region}.amazonaws.com/Prod/"
```

## Common Mistakes

### Wrong

```bash
# Deploying without building first
sam deploy --guided

# Building without container for native deps (numpy, pandas)
sam build
```

### Correct

```bash
# Always build before deploy
sam build --use-container && sam deploy --config-env dev

# Use --parallel for multi-function builds
sam build --use-container --parallel
```

## Key Flags

| Flag | Commands | Purpose |
|------|----------|---------|
| `--use-container` | build | Build inside Lambda-like Docker |
| `--parallel` | build | Build functions concurrently |
| `--guided` | deploy | Interactive first-time setup |
| `--config-env` | deploy, build | Select samconfig.toml section |
| `--no-confirm-changeset` | deploy | Skip changeset approval |
| `--watch` | sync | Auto-deploy on file changes |
| `--warm-containers` | local start-api | Reuse containers for speed |
| `--tail` | logs | Stream logs in real-time |

## SAM CLI Updates (2025-2026)

| Feature | Version | Description |
|---------|---------|-------------|
| **Finch support** | Oct 2025 | Use Finch as alternative to Docker for local dev |
| **SAM Accelerate (`sam sync`)** | GA | Hot-sync code changes in seconds, skip CloudFormation for code-only |
| **Package performance flag** | Merged Apr 2025 | `SAM_CLI_BETA_PACKAGE_PERFORMANCE` caches zips for shared code |
| **`sam build --watch`** | Proposed PR | Auto-rebuild on file changes (watchdog-based) |
| **Python 3.14 runtime** | Nov 2025 | `--runtime python3.14` for init and build |

```bash
# SAM Accelerate: instant code deployment (skip CloudFormation)
sam sync --stack-name my-stack --watch

# Use Finch instead of Docker (auto-detected if Docker unavailable)
sam build --use-container  # Works with Finch or Docker

# Package performance: cache zip for multi-function templates
SAM_CLI_BETA_PACKAGE_PERFORMANCE=1 sam deploy --config-env prod
```

## Related

- [AWS CLI](../concepts/aws-cli.md)
- [Environments](../concepts/environments.md)
- [SAM Deploy Pattern](../patterns/sam-deploy.md)
- [Local Testing](../patterns/local-testing.md)
