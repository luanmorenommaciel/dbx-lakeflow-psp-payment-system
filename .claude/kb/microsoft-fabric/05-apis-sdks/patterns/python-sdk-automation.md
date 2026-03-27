> **MCP Validated:** 2026-02-17

# Python SDK Automation

> **Purpose**: Comprehensive pattern for automating Fabric operations -- workspace provisioning, item deployment, and dataset refresh triggers

## When to Use

- Automating multi-environment workspace provisioning (dev, staging, prod)
- Deploying Fabric items (lakehouses, warehouses, pipelines) via CI/CD
- Triggering and monitoring dataset refreshes programmatically
- Building self-service provisioning portals for data teams

## Implementation

```python
"""Fabric automation SDK for workspace provisioning and item deployment."""
import time
import logging
from dataclasses import dataclass

import requests
from azure.identity import ClientSecretCredential

logger = logging.getLogger(__name__)

BASE_URL = "https://api.fabric.microsoft.com/v1"

@dataclass
class FabricConfig:
    """Configuration for Fabric automation."""
    tenant_id: str
    client_id: str
    client_secret: str
    capacity_id: str
    default_timeout: int = 600


class FabricAutomationClient:
    """Full-featured client for Fabric workspace and item automation."""

    def __init__(self, config: FabricConfig):
        self.config = config
        self.credential = ClientSecretCredential(
            tenant_id=config.tenant_id,
            client_id=config.client_id,
            client_secret=config.client_secret,
        )
        self._token = None

    def _get_headers(self) -> dict:
        if not self._token or self._token.expires_on < time.time():
            self._token = self.credential.get_token(
                "https://api.fabric.microsoft.com/.default"
            )
        return {
            "Authorization": f"Bearer {self._token.token}",
            "Content-Type": "application/json",
        }

    def _poll_lro(self, location_url: str) -> dict:
        """Poll long-running operation until completion."""
        deadline = time.time() + self.config.default_timeout
        while time.time() < deadline:
            resp = requests.get(location_url, headers=self._get_headers())
            result = resp.json()
            if result.get("status") in ("Succeeded", "Completed"):
                return result
            if result.get("status") in ("Failed", "Cancelled"):
                raise RuntimeError(f"LRO failed: {result}")
            time.sleep(int(resp.headers.get("Retry-After", 10)))
        raise TimeoutError("LRO timed out")

    # ── Workspace Provisioning ──────────────────────────────

    def provision_workspace(self, name: str, description: str = "") -> dict:
        """Create a workspace and assign it to a capacity."""
        payload = {
            "displayName": name,
            "capacityId": self.config.capacity_id,
            "description": description,
        }
        resp = requests.post(
            f"{BASE_URL}/workspaces",
            headers=self._get_headers(),
            json=payload,
        )
        resp.raise_for_status()
        ws = resp.json()
        logger.info(f"Created workspace '{name}' -> {ws['id']}")
        return ws

    def assign_workspace_role(
        self, workspace_id: str, principal_id: str,
        principal_type: str, role: str,
    ) -> None:
        """Assign a role (Admin, Member, Contributor, Viewer)."""
        payload = {
            "principal": {"id": principal_id, "type": principal_type},
            "role": role,
        }
        resp = requests.post(
            f"{BASE_URL}/workspaces/{workspace_id}/roleAssignments",
            headers=self._get_headers(),
            json=payload,
        )
        resp.raise_for_status()
        logger.info(f"Assigned {role} to {principal_id} in {workspace_id}")

    def deploy_lakehouse(self, workspace_id: str, name: str) -> dict:
        """Create a lakehouse in the specified workspace."""
        payload = {"displayName": name}
        resp = requests.post(
            f"{BASE_URL}/workspaces/{workspace_id}/lakehouses",
            headers=self._get_headers(), json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def deploy_warehouse(self, workspace_id: str, name: str) -> dict:
        """Create a warehouse (LRO -- returns after polling)."""
        resp = requests.post(
            f"{BASE_URL}/workspaces/{workspace_id}/warehouses",
            headers=self._get_headers(), json={"displayName": name},
        )
        if resp.status_code == 202:
            return self._poll_lro(resp.headers["Location"])
        resp.raise_for_status()
        return resp.json()

    def trigger_dataset_refresh(self, workspace_id: str, dataset_id: str) -> str:
        """Trigger a semantic model refresh; returns operation URL."""
        resp = requests.post(
            f"{BASE_URL}/workspaces/{workspace_id}"
            f"/items/{dataset_id}/jobs/instances?jobType=DatasetRefresh",
            headers=self._get_headers(),
        )
        resp.raise_for_status()
        return resp.headers.get("Location", "")
```

## Example Usage

```python
from fabric_automation import FabricAutomationClient, FabricConfig

config = FabricConfig(
    tenant_id="your-tenant-id",
    client_id="your-sp-client-id",
    client_secret="your-sp-secret",
    capacity_id="your-capacity-id",
)
client = FabricAutomationClient(config)

# 1. Provision a complete environment
ws = client.provision_workspace(
    name="analytics-prod",
    description="Production analytics workspace",
)

# 2. Deploy medallion lakehouses
for layer in ["bronze", "silver", "gold"]:
    lh = client.deploy_lakehouse(ws["id"], f"{layer}_lakehouse")
    print(f"Lakehouse {layer}: {lh['id']}")

# 3. Deploy a warehouse
wh = client.deploy_warehouse(ws["id"], "reporting_wh")
print(f"Warehouse: {wh['id']}")

# 4. Assign team roles
teams = [
    ("data-engineers-group-id", "Group", "Contributor"),
    ("analysts-group-id", "Group", "Viewer"),
    ("admin-user-id", "User", "Admin"),
]
for pid, ptype, role in teams:
    client.assign_workspace_role(ws["id"], pid, ptype, role)

# 5. Trigger dataset refresh
location = client.trigger_dataset_refresh(ws["id"], "dataset-id")
result = client.wait_for_refresh(location)
print(f"Refresh status: {result['status']}")
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `capacity_id` | Required | F or P SKU capacity for workspaces |
| `default_timeout` | 600s | Max wait for long-running operations |
| Rate limit | 200 req/min | Per-tenant API throttle |
| Retry-After | Header value | Seconds to wait on 429 |

## Related

- [REST API Fundamentals](../concepts/rest-api.md)
- [Fabric REST API v1](../concepts/fabric-rest-api.md)
- [SDK Automation](sdk-automation.md)
- [Power BI API](power-bi-api.md)
