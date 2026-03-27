# Secret Manager

> **Purpose**: Secure storage and versioned access for API keys, credentials, and configuration secrets
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Google Cloud Secret Manager stores API keys, passwords, certificates, and other sensitive
data as versioned secrets. Each secret can have multiple versions, enabling zero-downtime
rotation. Access is controlled via IAM with the `secretAccessor` role. In Cloud Run,
secrets can be mounted as environment variables or volumes at deploy time.

## The Pattern

```python
from google.cloud import secretmanager

client = secretmanager.SecretManagerServiceClient()

def access_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    """Access a secret version and return the payload as a string."""
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")

def create_secret(project_id: str, secret_id: str, payload: str) -> str:
    """Create a new secret with an initial version."""
    parent = f"projects/{project_id}"
    secret = client.create_secret(
        request={
            "parent": parent,
            "secret_id": secret_id,
            "secret": {"replication": {"automatic": {}}},
        }
    )
    version = client.add_secret_version(
        request={
            "parent": secret.name,
            "payload": {"data": payload.encode("utf-8")},
        }
    )
    return version.name

def add_version(project_id: str, secret_id: str, payload: str) -> str:
    """Add a new version to an existing secret."""
    parent = f"projects/{project_id}/secrets/{secret_id}"
    version = client.add_secret_version(
        request={
            "parent": parent,
            "payload": {"data": payload.encode("utf-8")},
        }
    )
    return version.name
```

## Quick Reference

| Input | Output | Notes |
|-------|--------|-------|
| `access_secret_version(name)` | `SecretPayload` | Decode `.payload.data` |
| `create_secret(parent, secret_id)` | `Secret` resource | Empty until version added |
| `add_secret_version(parent, payload)` | `SecretVersion` | Immutable once created |
| Version `"latest"` | Most recent version | Avoid in production |

## Cloud Run Integration

```bash
# Mount secret as environment variable at deploy time
gcloud run deploy my-service \
    --set-secrets="API_KEY=my-api-key:3" \
    --set-secrets="DB_PASSWORD=db-pass:latest"

# Mount secret as file volume
gcloud run deploy my-service \
    --set-secrets="/secrets/api-key=my-api-key:3"
```

## Common Mistakes

### Wrong

```python
# Using "latest" in production (unpredictable after rotation)
secret_value = access_secret("my-project", "api-key", "latest")

# Printing secrets to logs
print(f"API Key: {secret_value}")  # Never do this
```

### Correct

```python
# Pin to specific version number in production
secret_value = access_secret("my-project", "api-key", "3")

# Use secrets without logging them
import os
api_key = os.environ.get("API_KEY")  # Mounted by Cloud Run
# Use api_key directly in API calls, never print
```

## Related

- [IAM](../concepts/iam.md) - Requires `roles/secretmanager.secretAccessor`
- [Cloud Run](../concepts/cloud-run.md) - Mounts secrets as env vars or volumes
- [Event-Driven Pipeline](../patterns/event-driven-pipeline.md) - Secrets in pipeline config
