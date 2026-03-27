> **MCP Validated:** 2026-02-17

# Deployment Pipelines

> **Purpose**: CI/CD deployment patterns using Fabric deployment pipelines and Azure DevOps

## When to Use

- Promoting Fabric items across Dev, Test, and Prod environments
- Automating deployments via REST API from Azure DevOps or GitHub Actions
- Implementing approval gates and validation checks before production
- Managing environment-specific configurations (connections, parameters)

## Implementation

```python
"""Fabric deployment pipeline automation with Azure DevOps integration."""
import requests
import time
from typing import Optional

class DeploymentPipelineManager:
    """Manage Fabric deployment pipelines programmatically."""

    BASE_URL = "https://api.fabric.microsoft.com/v1"

    def __init__(self, headers: dict):
        self.headers = headers

    def create_deployment_pipeline(self, name: str) -> dict:
        """Create a new deployment pipeline."""
        payload = {"displayName": name, "description": f"CI/CD for {name}"}
        resp = requests.post(
            f"{self.BASE_URL}/deploymentPipelines",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()
        return resp.json()

    def assign_stage(
        self, pipeline_id: str, stage_order: int, workspace_id: str,
    ) -> None:
        """Assign a workspace to a deployment pipeline stage."""
        payload = {"workspaceId": workspace_id}
        resp = requests.post(
            f"{self.BASE_URL}/deploymentPipelines/{pipeline_id}"
            f"/stages/{stage_order}/assignWorkspace",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()

    def deploy(
        self, pipeline_id: str, source_stage: int,
        note: Optional[str] = None,
        items: Optional[list[dict]] = None,
    ) -> str:
        """Deploy from one stage to the next."""
        payload = {
            "sourceStageOrder": source_stage,
            "isBackwardDeployment": False,
            "newDeploymentNote": note or "Automated deployment",
        }
        if items:
            payload["items"] = items  # Selective deployment
        # else deploys all items

        resp = requests.post(
            f"{self.BASE_URL}/deploymentPipelines/{pipeline_id}/deploy",
            headers=self.headers, json=payload,
        )
        resp.raise_for_status()
        return resp.headers.get("Location", "")

    def wait_for_deployment(self, location_url: str, timeout: int = 300) -> dict:
        """Poll deployment status until completion."""
        start = time.time()
        while time.time() - start < timeout:
            resp = requests.get(location_url, headers=self.headers)
            result = resp.json()
            status = result.get("status", "Unknown")
            if status in ("Succeeded", "Failed", "Cancelled"):
                return result
            time.sleep(5)
        raise TimeoutError("Deployment did not complete in time")

    def get_stages(self, pipeline_id: str) -> list[dict]:
        """Get all stages and their items."""
        resp = requests.get(
            f"{self.BASE_URL}/deploymentPipelines/{pipeline_id}/stages",
            headers=self.headers,
        )
        resp.raise_for_status()
        return resp.json()["value"]
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Stages | 3 (Dev/Test/Prod) | Up to 10 stages supported |
| Deployment scope | All items | Can filter specific items |
| Deployment rules | None | Parameter overrides per stage |
| Backward deploy | Disabled | Can deploy from higher to lower |
| Approval gates | Manual | Built-in stage approval |

## Example Usage

```yaml
# Azure DevOps pipeline: azure-pipelines.yml
trigger:
  branches:
    include:
      - main

pool:
  vmImage: 'ubuntu-latest'

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.11'

  - script: pip install requests azure-identity
    displayName: 'Install dependencies'

  - task: AzureCLI@2
    displayName: 'Deploy Dev to Test'
    inputs:
      azureSubscription: 'fabric-service-connection'
      scriptType: 'bash'
      scriptLocation: 'inlineScript'
      inlineScript: |
        python -c "
        from azure.identity import DefaultAzureCredential
        import requests

        cred = DefaultAzureCredential()
        token = cred.get_token('https://api.fabric.microsoft.com/.default')
        headers = {'Authorization': f'Bearer {token.token}', 'Content-Type': 'application/json'}

        pipeline_id = '$(FABRIC_PIPELINE_ID)'
        url = f'https://api.fabric.microsoft.com/v1/deploymentPipelines/{pipeline_id}/deploy'
        payload = {'sourceStageOrder': 0, 'newDeploymentNote': 'Build $(Build.BuildNumber)'}
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        print(f'Deployment initiated: {resp.status_code}')
        "
```

## Deployment Strategies

| Strategy | Description | Best For |
|----------|-------------|----------|
| Git-centric | All deploys from Git (branch per stage) | Full version control |
| Hybrid | Git for Dev, pipelines for Test/Prod | Balanced governance |
| Pipeline-only | Built-in deployment pipelines | Simple environments |
| API-driven | REST API from external CI/CD | Custom workflows |

## See Also

- [Git Integration](../concepts/git-integration.md)
- [SDK Automation](../../05-apis-sdks/patterns/sdk-automation.md)
- [REST API](../../05-apis-sdks/concepts/rest-api.md)
