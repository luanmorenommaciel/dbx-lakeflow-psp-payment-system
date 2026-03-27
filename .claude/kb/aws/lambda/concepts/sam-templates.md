# SAM Templates

> **Purpose**: AWS SAM (Serverless Application Model) template structure for Lambda deployment
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

AWS SAM is an extension of CloudFormation that simplifies defining serverless applications.
A SAM template declares Lambda functions, API gateways, event sources, IAM roles, and
layers in a single YAML file. SAM CLI builds, tests locally, and deploys the stack.

## The Pattern

```yaml
AWSTemplateFormatVersion: "2010-09-09"
Transform: AWS::Serverless-2016-10-31
Description: S3 file processing Lambda

Globals:
  Function:
    Runtime: python3.12
    Timeout: 300
    MemorySize: 512
    Architectures:
      - x86_64
    Environment:
      Variables:
        LOG_LEVEL: INFO
        POWERTOOLS_SERVICE_NAME: file-processor

Resources:
  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: src/
      Handler: app.lambda_handler
      Description: Process files uploaded to S3
      Layers:
        - !Sub arn:aws:lambda:${AWS::Region}:336392948345:layer:AWSSDKPandas-Python312:15
      Policies:
        - S3ReadPolicy:
            BucketName: !Ref InputBucket
        - S3CrudPolicy:
            BucketName: !Ref OutputBucket
      Events:
        S3Upload:
          Type: S3
          Properties:
            Bucket: !Ref InputBucket
            Events: s3:ObjectCreated:*
            Filter:
              S3Key:
                Rules:
                  - Name: suffix
                    Value: .csv
```

## Quick Reference

| Section | Purpose | Required |
|---------|---------|----------|
| `Transform` | Declares SAM template | Yes |
| `Globals` | Shared function config | No (recommended) |
| `Resources` | Functions, buckets, tables | Yes |
| `Parameters` | Input variables | No |
| `Outputs` | Exported values (ARNs, URLs) | No |
| `Conditions` | Conditional resource creation | No |

## SAM Policy Templates

| Policy Template | Grants | Use Case |
|-----------------|--------|----------|
| `S3ReadPolicy` | s3:GetObject, s3:ListBucket | Read input files |
| `S3CrudPolicy` | Full S3 CRUD on bucket | Write output files |
| `S3WritePolicy` | s3:PutObject only | Write-only access |
| `SQSSendMessagePolicy` | sqs:SendMessage | Send to DLQ |
| `CloudWatchLogsFullAccess` | Full CW Logs | Logging (auto-added) |
| `SSMParameterReadPolicy` | ssm:GetParameter | Read config from SSM |

## Common Mistakes

### Wrong (no filter, overly broad)

```yaml
Events:
  S3Upload:
    Type: S3
    Properties:
      Bucket: !Ref MyBucket
      Events: s3:ObjectCreated:*
      # No filter = triggers on ALL uploads including outputs!
```

### Correct (filtered by prefix and suffix)

```yaml
Events:
  S3Upload:
    Type: S3
    Properties:
      Bucket: !Ref InputBucket
      Events: s3:ObjectCreated:*
      Filter:
        S3Key:
          Rules:
            - Name: prefix
              Value: raw/
            - Name: suffix
              Value: .csv
```

## Deploy Workflow

```bash
# First deploy (interactive, creates samconfig.toml)
sam build && sam deploy --guided

# Subsequent deploys (uses saved config)
sam build && sam deploy

# Deploy to specific environment
sam deploy --config-env production
```

## Related

- [Lambda Handler](../concepts/lambda-handler.md)
- [S3 Triggers](../concepts/s3-triggers.md)
- [IAM Policies](../concepts/iam-policies.md)
- [SAM Template Spec](../specs/sam-template.yaml)
