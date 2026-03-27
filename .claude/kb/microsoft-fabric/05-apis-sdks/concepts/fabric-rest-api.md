> **MCP Validated:** 2026-02-17

# Fabric REST API v1

> **Purpose**: Comprehensive reference for Fabric REST API v1 endpoints, authentication, pagination, and long-running operations
> **Confidence**: 0.95

## Overview

The Microsoft Fabric REST API v1 provides programmatic access to all Fabric resources through `https://api.fabric.microsoft.com/v1`. Authentication requires Entra ID (Azure AD) OAuth2 bearer tokens with the scope `https://api.fabric.microsoft.com/.default`. The API supports user principals, service principals, and managed identities. All responses use JSON and follow OData conventions for pagination via `continuationUri`.

## Authentication

### Entra ID Token Acquisition

```bash
# curl: Get token via client credentials flow
curl -X POST "https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "client_id={client_id}" \
  -d "client_secret={client_secret}" \
  -d "scope=https://api.fabric.microsoft.com/.default" \
  -d "grant_type=client_credentials"
```

```python
from azure.identity import ClientSecretCredential, InteractiveBrowserCredential

# Service principal (automation)
credential = ClientSecretCredential(
    tenant_id="your-tenant-id",
    client_id="your-client-id",
    client_secret="your-client-secret",
)
token = credential.get_token("https://api.fabric.microsoft.com/.default")

# Interactive user (development)
credential = InteractiveBrowserCredential()
token = credential.get_token("https://api.fabric.microsoft.com/.default")
```

## API Scopes

| Scope | Description |
|-------|-------------|
| `Workspace.ReadWrite.All` | Full workspace management |
| `Item.ReadWrite.All` | Create, update, delete items |
| `Item.Execute.All` | Run jobs (pipelines, notebooks) |
| `OneLake.ReadWrite.All` | OneLake file operations |
| `Dataset.ReadWrite.All` | Semantic model management |
| `Capacity.ReadWrite.All` | Capacity administration |

## Key Endpoints

```bash
# List workspaces
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.fabric.microsoft.com/v1/workspaces"

# Get workspace items
curl -H "Authorization: Bearer $TOKEN" \
  "https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"

# Create a lakehouse
curl -X POST -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"displayName": "bronze_lakehouse"}' \
  "https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"

# Run a notebook job
curl -X POST -H "Authorization: Bearer $TOKEN" \
  "https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items/{item_id}/jobs/instances?jobType=RunNotebook"
```

## Pagination

```python
import requests

def get_all_pages(url: str, headers: dict) -> list[dict]:
    """Handle Fabric API pagination with continuationUri."""
    results = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        results.extend(data.get("value", []))
        url = data.get("continuationUri")  # None when no more pages
    return results

# Usage
all_items = get_all_pages(
    f"https://api.fabric.microsoft.com/v1/workspaces/{ws_id}/items",
    headers={"Authorization": f"Bearer {token.token}"},
)
```

## Long-Running Operations (LRO)

```python
import time
import requests

def poll_lro(location_url: str, headers: dict, timeout: int = 300) -> dict:
    """Poll a long-running operation until completion."""
    start = time.time()
    while time.time() - start < timeout:
        resp = requests.get(location_url, headers=headers)
        result = resp.json()
        status = result.get("status")
        if status in ("Succeeded", "Completed"):
            return result
        if status in ("Failed", "Cancelled"):
            raise RuntimeError(f"LRO failed: {result}")
        retry_after = int(resp.headers.get("Retry-After", 10))
        time.sleep(retry_after)
    raise TimeoutError(f"LRO not complete after {timeout}s")

# Example: Create warehouse returns 202 with Location header
resp = requests.post(
    f"{BASE_URL}/workspaces/{ws_id}/warehouses",
    headers=headers,
    json={"displayName": "analytics_wh"},
)
if resp.status_code == 202:
    result = poll_lro(resp.headers["Location"], headers)
```

## Rate Limits

| Limit Type | Value | Response |
|------------|-------|----------|
| Per-tenant | 200 requests/min | 429 Too Many Requests |
| Retry-After | Header value in seconds | Back off accordingly |
| Burst | 50 requests/10s | 429 with Retry-After |

## Related

- [REST API Fundamentals](rest-api.md)
- [SDK Automation](../patterns/sdk-automation.md)
- [Python SDK Automation](../patterns/python-sdk-automation.md)
