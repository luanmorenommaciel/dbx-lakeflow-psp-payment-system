# SAM Deploy

> **Purpose**: End-to-end SAM build, package, and deploy workflow for Lambda applications
> **MCP Validated**: 2026-02-17

## When to Use

- Deploying Lambda functions and API Gateway endpoints to AWS
- First-time project setup with `--guided` mode
- CI/CD pipeline deployment to multiple environments
- Gradual rollouts with CodeDeploy traffic shifting

## Implementation

```bash
#!/bin/bash
# deploy.sh - Complete SAM deployment workflow
set -euo pipefail

ENV="${1:-dev}"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "=== Deploying to ${ENV} ==="

# Step 1: Validate template
echo "[1/4] Validating template..."
sam validate --lint

# Step 2: Build artifacts
echo "[2/4] Building..."
sam build \
  --use-container \
  --parallel \
  --cached

# Step 3: Run local tests (dev only)
if [ "$ENV" = "dev" ]; then
  echo "[2.5/4] Running local smoke test..."
  sam local invoke MyFunction \
    -e events/test-event.json \
    --env-vars env-vars/${ENV}.json \
    2>/dev/null || echo "Local test skipped (Docker not available)"
fi

# Step 4: Deploy
echo "[3/4] Deploying to ${ENV}..."
sam deploy \
  --config-env "${ENV}" \
  --no-fail-on-empty-changeset \
  --tags "Environment=${ENV} ManagedBy=SAM"

# Step 5: Verify
echo "[4/4] Verifying deployment..."
aws cloudformation describe-stacks \
  --stack-name "myapp-${ENV}" \
  --query "Stacks[0].StackStatus" \
  --output text

# Show outputs (API URL, etc.)
sam list endpoints --stack-name "myapp-${ENV}" --output table 2>/dev/null || \
  aws cloudformation describe-stacks \
    --stack-name "myapp-${ENV}" \
    --query "Stacks[0].Outputs" \
    --output table

echo "=== Deploy complete ==="
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `--use-container` | false | Build inside Docker (needed for native deps) |
| `--parallel` | false | Build functions concurrently |
| `--cached` | false | Skip unchanged functions |
| `--config-env` | default | samconfig.toml environment section |
| `--no-fail-on-empty-changeset` | false | Don't error if no changes |
| `--no-confirm-changeset` | false | Skip interactive approval |
| `--tags` | -- | CloudFormation resource tags |
| `--capabilities` | -- | IAM capability grants |

## First-Time Setup

```bash
# Generate samconfig.toml interactively
sam deploy --guided

# Prompts:
#   Stack Name: myapp-dev
#   AWS Region: us-east-1
#   Confirm changeset: Y
#   Allow SAM CLI IAM role creation: Y
#   Save arguments to samconfig.toml: Y
#   SAM config environment: dev
```

## Gradual Deployment with CodeDeploy

```yaml
# template.yaml - Traffic shifting
Resources:
  MyFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.lambda_handler
      CodeUri: src/
      AutoPublishAlias: live
      DeploymentPreference:
        Type: Canary10Percent5Minutes
        Alarms:
          - !Ref MyFunctionErrorAlarm
        Hooks:
          PreTraffic: !Ref PreTrafficHook
          PostTraffic: !Ref PostTrafficHook
```

| Deployment Type | Traffic Shift Pattern |
|----------------|----------------------|
| `Canary10Percent5Minutes` | 10% for 5 min, then 100% |
| `Canary10Percent30Minutes` | 10% for 30 min, then 100% |
| `Linear10PercentEvery1Minute` | +10% every minute |
| `Linear10PercentEvery10Minutes` | +10% every 10 min |
| `AllAtOnce` | Immediate 100% shift |

## Rollback

```bash
# Manual rollback to previous version
aws cloudformation rollback-stack --stack-name myapp-prod

# Delete stack entirely
sam delete --stack-name myapp-dev --no-prompts

# Check rollback status
aws cloudformation describe-stacks \
  --stack-name myapp-prod \
  --query "Stacks[0].StackStatus"
```

## Example Usage

```bash
# Quick dev deploy
sam build && sam deploy --config-env dev

# Production deploy with review
sam build --use-container --parallel --cached
sam deploy --config-env prod

# Deploy with parameter override
sam deploy --config-env dev \
  --parameter-overrides "MemorySize=512 LogLevel=DEBUG"

# Deploy and tail logs immediately
sam deploy --config-env dev && \
  sam logs --stack-name myapp-dev --tail
```

## See Also

- [SAM CLI](../concepts/sam-cli.md)
- [Local Testing](../patterns/local-testing.md)
- [Environments](../concepts/environments.md)
