> **MCP Validated:** 2026-02-17

# Python SDK Automation

> **Purpose**: Automating Fabric workspace, item, and pipeline management with Python and REST APIs

## When to Use

- Provisioning workspaces and items programmatically across environments
- Automating bulk operations (create multiple lakehouses, assign permissions)
- Building custom CI/CD workflows beyond built-in deployment pipelines
- Integrating Fabric management into existing Python-based DevOps tooling

## Implementation

```python
"""Fabric automation client for workspace and item management."""
import requests
import time
from typing import Optional
from azure.identity import ClientSecretCredential, DefaultAzureCredential

class FabricClient:
    """Production-ready client for Microsoft Fabric REST API."""

    BASE_URL = "https://api.fabric.microsoft.com/v1"

    def __init__(self, tenant_id: str, client_id: str, client_secret: str):
        self.credential = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
        )
        self._token = None

    @property
    def headers(self) -> dict:
        if not self._token:
            self._token = self.credential.get_token(
                "https://api.fabric.microsoft.com/.default"
            )
        return {
            "Authorization": f"Bearer {self._token.token}",
            "Content-Type": "application/json",
        }

    def list_workspaces(self) -> list[dict]:
        resp = requests.get(f"{self.BASE_URL}/workspaces", headers=self.headers)
        resp.raise_for_status()
        return resp.json()["value"]

    def create_workspace(self, name: str, capacity_id: str) -> dict:
        payload = {"displayName": name, "capacityId": capacity_id}
        resp = requests.post(
            f"{self.BASE_URL}/workspaces",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def create_lakehouse(self, workspace_id: str, name: str) -> dict:
        payload = {"displayName": name}
        resp = requests.post(
            f"{self.BASE_URL}/workspaces/{workspace_id}/lakehouses",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def run_pipeline(
        self, workspace_id: str, pipeline_id: str,
        params: Optional[dict] = None,
    ) -> str:
        payload = {}
        if params:
            payload["executionData"] = {"parameters": params}
        resp = requests.post(
            f"{self.BASE_URL}/workspaces/{workspace_id}"
            f"/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()
        location = resp.headers.get("Location", "")
        return location  # Poll this URL for status

    def wait_for_job(self, location_url: str, timeout: int = 600) -> dict:
        start = time.time()
        while time.time() - start < timeout:
            resp = requests.get(location_url, headers=self.headers)
            result = resp.json()
            if result.get("status") in ("Completed", "Failed", "Cancelled"):
                return result
            time.sleep(10)
        raise TimeoutError(f"Job did not complete within {timeout}s")

    def assign_workspace_role(
        self, workspace_id: str, principal_id: str,
        principal_type: str, role: str,
    ) -> None:
        payload = {
            "principal": {"id": principal_id, "type": principal_type},
            "role": role,
        }
        resp = requests.post(
            f"{self.BASE_URL}/workspaces/{workspace_id}/roleAssignments",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| API base URL | `https://api.fabric.microsoft.com/v1` | Fabric REST endpoint |
| Token scope | `https://api.fabric.microsoft.com/.default` | OAuth2 scope |
| Rate limit | 200 req/min | Per-tenant throttle |
| Long-running ops | Location header | Poll for completion |
| Retry strategy | Exponential backoff | On 429/5xx responses |

## Example Usage

```python
# Provision a complete environment
client = FabricClient(
    tenant_id="your-tenant-id",
    client_id="your-sp-client-id",
    client_secret="your-sp-secret",
)

# Create workspace with capacity
ws = client.create_workspace("analytics-prod", capacity_id="cap-id")

# Create medallion lakehouses
for layer in ["bronze", "silver", "gold"]:
    lh = client.create_lakehouse(ws["id"], f"{layer}_lakehouse")
    print(f"Created {layer}: {lh['id']}")

# Assign contributor role
client.assign_workspace_role(
    workspace_id=ws["id"],
    principal_id="user-or-group-id",
    principal_type="Group",
    role="Contributor",
)
```

## See Also

- [REST API](../concepts/rest-api.md)
- [Deployment Pipelines](../../07-cicd-automation/patterns/deployment-pipelines.md)
- [Git Integration](../../07-cicd-automation/concepts/git-integration.md)
