> **MCP Validated:** 2026-02-17

# Fabric REST APIs

> **Purpose**: Microsoft Fabric REST API fundamentals for programmatic platform management
> **Confidence**: 0.95

## Overview

Microsoft Fabric exposes a comprehensive REST API for managing workspaces, items (lakehouses, warehouses, pipelines), deployments, and Git integration. All endpoints use the base URL `https://api.fabric.microsoft.com/v1` and require Azure AD (Entra ID) authentication via OAuth2 bearer tokens. The API supports both user principal and service principal authentication.

## The Pattern

```python
import requests
from azure.identity import ClientSecretCredential

# Authenticate with service principal
credential = ClientSecretCredential(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret"
)
token = credential.get_token("https://api.fabric.microsoft.com/.default")

headers = {
    "Authorization": f"Bearer {token.token}",
    "Content-Type": "application/json"
}

BASE_URL = "https://api.fabric.microsoft.com/v1"

# List all workspaces
response = requests.get(f"{BASE_URL}/workspaces", headers=headers)
workspaces = response.json()["value"]

# Get items in a workspace
workspace_id = workspaces[0]["id"]
items = requests.get(
    f"{BASE_URL}/workspaces/{workspace_id}/items",
    headers=headers
).json()["value"]

# Create a new lakehouse
lakehouse_payload = {
    "displayName": "bronze_lakehouse",
    "type": "Lakehouse"
}
result = requests.post(
    f"{BASE_URL}/workspaces/{workspace_id}/lakehouses",
    headers=headers,
    json=lakehouse_payload
)
print(f"Created: {result.json()['id']}")
```

## Quick Reference

| Endpoint Category | Base Path | Operations |
|-------------------|-----------|------------|
| Workspaces | `/v1/workspaces` | CRUD, role assignments |
| Lakehouses | `/v1/workspaces/{id}/lakehouses` | CRUD, table management |
| Warehouses | `/v1/workspaces/{id}/warehouses` | CRUD |
| Pipelines | `/v1/workspaces/{id}/dataPipelines` | CRUD, run jobs |
| Deployment | `/v1/deploymentPipelines` | Stage management, deploy |
| Git | `/v1/workspaces/{id}/git` | Connect, commit, update |
| Items (generic) | `/v1/workspaces/{id}/items` | List, get by type |
| Admin | `/v1/admin/workspaces` | Tenant-wide operations |

## Common Mistakes

### Wrong

```python
# Using Azure Management API instead of Fabric API
url = "https://management.azure.com/subscriptions/.../fabric"
```

### Correct

```python
# Fabric has its own dedicated API
url = "https://api.fabric.microsoft.com/v1/workspaces"
# Token scope: https://api.fabric.microsoft.com/.default
```

## Authentication Scopes

| Auth Type | Scope | Use Case |
|-----------|-------|----------|
| User principal | `https://api.fabric.microsoft.com/.default` | Interactive scripts |
| Service principal | `https://api.fabric.microsoft.com/.default` | CI/CD automation |
| Managed identity | `https://api.fabric.microsoft.com/.default` | Azure-hosted services |

## Related

- [SDK Automation](../patterns/sdk-automation.md)
- [Git Integration](../../07-cicd-automation/concepts/git-integration.md)
- [Deployment Pipelines](../../07-cicd-automation/patterns/deployment-pipelines.md)
