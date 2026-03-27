# Local Testing

> **Purpose**: Test Lambda functions and API endpoints locally using SAM CLI and Docker
> **MCP Validated**: 2026-02-17

## When to Use

- Testing Lambda handler logic before deploying to AWS
- Debugging API Gateway integrations locally
- Validating event payloads against function handlers
- Rapid iteration without incurring AWS costs

## Implementation

```bash
#!/bin/bash
# local-test.sh - Comprehensive local testing workflow
set -euo pipefail

FUNCTION="${1:-MyFunction}"
EVENT_FILE="${2:-events/test-event.json}"
ENV_FILE="${3:-env-vars/dev.json}"

echo "=== Local Testing: ${FUNCTION} ==="

# Step 1: Build first (required for local testing)
sam build --cached

# Step 2: Invoke function with test event
echo "[invoke] Testing ${FUNCTION} with ${EVENT_FILE}..."
sam local invoke "${FUNCTION}" \
  -e "${EVENT_FILE}" \
  --env-vars "${ENV_FILE}" \
  --log-file /tmp/sam-local.log

echo "[invoke] Response above. Logs at /tmp/sam-local.log"

# Step 3: Start local API (background)
echo "[api] Starting local API Gateway on port 3000..."
sam local start-api \
  --port 3000 \
  --warm-containers EAGER \
  --env-vars "${ENV_FILE}"
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `--port` | 3000 | Local API port number |
| `--warm-containers` | -- | EAGER or LAZY container reuse |
| `--env-vars` | -- | JSON file with environment variables |
| `--docker-network` | -- | Connect to existing Docker network |
| `--skip-pull-image` | false | Use cached Docker images |
| `--debug-port` | -- | Port for step-through debugging |
| `--log-file` | -- | File to write Lambda logs |
| `--layer-cache-basedir` | -- | Cache directory for Lambda layers |

## Generate Test Events

```bash
# Generate sample events for different AWS services
sam local generate-event s3 put \
  --bucket my-bucket \
  --key data/file.csv > events/s3-put.json

sam local generate-event apigateway aws-proxy \
  --method GET \
  --path /hello \
  --body '{"key":"value"}' > events/api-get.json

sam local generate-event sqs receive-message \
  --body '{"order_id": "123"}' > events/sqs-message.json

sam local generate-event dynamodb update \
  > events/dynamodb-update.json

sam local generate-event cloudwatch scheduled-event \
  > events/scheduled.json
```

## Environment Variables File

```json
{
  "MyFunction": {
    "ENV": "dev",
    "LOG_LEVEL": "DEBUG",
    "DB_TABLE": "myapp-dev-table",
    "S3_BUCKET": "myapp-dev-data",
    "API_KEY": "test-key-local"
  },
  "AnotherFunction": {
    "ENV": "dev",
    "QUEUE_URL": "http://localhost:4566/queue/test"
  }
}
```

## Local API Testing

```bash
# Start local API with hot reload
sam local start-api --port 3000 --warm-containers EAGER

# In another terminal, test endpoints
curl http://localhost:3000/hello
curl -X POST http://localhost:3000/items \
  -H "Content-Type: application/json" \
  -d '{"name": "test-item", "price": 29.99}'

# Test with query parameters
curl "http://localhost:3000/search?q=test&limit=10"

# Test with path parameters
curl http://localhost:3000/items/item-123
```

## Debugging with IDE

```bash
# Start with debug port for VS Code / PyCharm
sam local invoke MyFunction \
  -e events/test-event.json \
  --debug-port 5858

# VS Code launch.json for attaching
# {
#   "type": "python",
#   "request": "attach",
#   "name": "SAM Local Debug",
#   "port": 5858,
#   "host": "localhost",
#   "pathMappings": [{
#     "localRoot": "${workspaceFolder}/src",
#     "remoteRoot": "/var/task"
#   }]
# }
```

## Docker Tips

```bash
# Use --skip-pull-image to avoid re-downloading runtime images
sam local invoke MyFunction --skip-pull-image -e events/test.json

# Connect to local services (e.g., LocalStack, local DB)
sam local invoke MyFunction --docker-network host

# Clean up SAM Docker containers
docker ps -a --filter "name=sam" -q | xargs docker rm -f 2>/dev/null
```

## Common Mistakes

### Wrong

```bash
# Testing without building first
sam local invoke MyFunction -e event.json
# Error: Function not found or build artifacts missing

# Forgetting env vars for functions that need them
sam local invoke MyFunction -e event.json
# Error: KeyError 'DB_TABLE' in Lambda handler
```

### Correct

```bash
# Always build before local testing
sam build --cached && sam local invoke MyFunction \
  -e events/test.json \
  --env-vars env-vars/dev.json
```

## Example Usage

```bash
# Quick single function test
sam build --cached && sam local invoke -e events/api-get.json

# Full API test session
sam build && sam local start-api --port 3000 --warm-containers EAGER

# Test with specific Docker network
sam local invoke --docker-network my-network -e events/test.json

# Generate and immediately test with S3 event
sam local generate-event s3 put --bucket test --key data.csv | \
  sam local invoke S3ProcessorFunction
```

## See Also

- [SAM CLI](../concepts/sam-cli.md)
- [SAM Deploy](../patterns/sam-deploy.md)
- [Environments](../concepts/environments.md)
