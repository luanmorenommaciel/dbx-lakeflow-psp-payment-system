# IAM Policies

> **Purpose**: Least-privilege IAM execution roles and policies for Lambda functions
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Every Lambda function assumes an IAM execution role that defines what AWS services it
can access. The principle of least privilege means granting only the exact permissions
needed. SAM provides policy templates for common patterns. For production, use IAM
Access Analyzer to refine policies based on actual CloudTrail usage.

## The Pattern

```yaml
# SAM template using policy templates (recommended)
Resources:
  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.lambda_handler
      Runtime: python3.12
      Policies:
        # Read from input bucket
        - S3ReadPolicy:
            BucketName: !Ref InputBucket
        # Write to output bucket
        - S3CrudPolicy:
            BucketName: !Ref OutputBucket
        # Send failed events to DLQ
        - SQSSendMessagePolicy:
            QueueName: !GetAtt DeadLetterQueue.QueueName
```

## Custom Policy (when SAM templates are insufficient)

```yaml
Policies:
  - Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - s3:GetObject
          - s3:HeadObject
        Resource: !Sub "arn:aws:s3:::${InputBucket}/raw/*"
      - Effect: Allow
        Action:
          - s3:PutObject
        Resource: !Sub "arn:aws:s3:::${OutputBucket}/processed/*"
      - Effect: Allow
        Action:
          - s3:ListBucket
        Resource:
          - !Sub "arn:aws:s3:::${InputBucket}"
          - !Sub "arn:aws:s3:::${OutputBucket}"
```

## Quick Reference

| SAM Policy Template | Actions Granted | Resource |
|---------------------|-----------------|----------|
| `S3ReadPolicy` | GetObject, ListBucket | Specific bucket |
| `S3WritePolicy` | PutObject | Specific bucket |
| `S3CrudPolicy` | Get, Put, Delete, List | Specific bucket |
| `SQSSendMessagePolicy` | SendMessage | Specific queue |
| `SNSPublishMessagePolicy` | Publish | Specific topic |
| `DynamoDBCrudPolicy` | Full CRUD | Specific table |
| `SSMParameterReadPolicy` | GetParameter, GetParameters | Specific param |
| `KMSDecryptPolicy` | Decrypt | Specific key |

## Common Mistakes

### Wrong (overly broad permissions)

```yaml
Policies:
  - Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action: "s3:*"           # Too broad
        Resource: "*"             # All buckets!
```

### Correct (least privilege)

```yaml
Policies:
  - Version: "2012-10-17"
    Statement:
      - Effect: Allow
        Action:
          - s3:GetObject         # Only what's needed
        Resource:
          - !Sub "arn:aws:s3:::${InputBucket}/raw/*"  # Scoped to prefix
```

## Auto-Generated Permissions

SAM automatically adds these permissions (do not duplicate):
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents` (CloudWatch)
- S3 invoke permission when using S3 event trigger

## Production Hardening

```yaml
# Use conditions to restrict access further
Statement:
  - Effect: Allow
    Action:
      - s3:GetObject
    Resource: !Sub "arn:aws:s3:::${InputBucket}/raw/*"
    Condition:
      StringEquals:
        s3:ExistingObjectTag/environment: production
```

## IAM Access Analyzer Workflow

```text
1. Deploy with broader permissions during development
2. Run function with real workloads for 30-90 days
3. Use IAM Access Analyzer to generate least-privilege policy
4. Replace development policy with generated policy
5. Test thoroughly before production deployment
```

## Related

- [SAM Templates](../concepts/sam-templates.md)
- [Lambda Handler](../concepts/lambda-handler.md)
- [Error Handling](../patterns/error-handling.md)
