> **MCP Validated:** 2026-02-17

# Azure DevOps Pipeline for Fabric CI/CD

> **Purpose**: Complete Azure DevOps YAML pipeline pattern for automated Fabric deployments using REST API and Service Principal auth

## When to Use

- Automating Fabric deployment pipelines from Azure DevOps
- Triggering deployments on branch merge or PR completion
- Enforcing validation checks before promoting to production

## Overview

This pattern provides a complete `azure-pipelines.yml` that authenticates via Service Principal, deploys through Fabric's deployment pipeline REST API, and polls for completion. It supports multi-stage promotion (Dev to Test to Prod) with approval gates managed in Azure DevOps environments.

## Prerequisites

| Requirement | Detail |
|-------------|--------|
| Service Principal | App registration with Fabric API permissions |
| ADO Service Connection | Federated or secret-based SP connection |
| Fabric pipeline | Pre-created with stages and workspace bindings |
| Pipeline variables | `FABRIC_PIPELINE_ID` |

## Implementation

```yaml
# azure-pipelines.yml -- Fabric CI/CD Deployment
trigger:
  branches:
    include:
      - main
      - release/*

pr:
  branches:
    include:
      - main

variables:
  - group: fabric-deployment-vars  # Contains FABRIC_PIPELINE_ID
  - name: pythonVersion
    value: '3.11'

stages:
  # ── Stage 1: Validate ────────────────────────────────────
  - stage: Validate
    displayName: 'Validate Fabric Items'
    jobs:
      - job: ValidateItems
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '$(pythonVersion)'

          - script: pip install requests azure-identity
            displayName: 'Install dependencies'

          - task: AzureCLI@2
            displayName: 'Check deployment pipeline status'
            inputs:
              azureSubscription: 'fabric-service-connection'
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                python scripts/fabric_validate.py \
                  --pipeline-id $(FABRIC_PIPELINE_ID)

  # ── Stage 2: Deploy Dev to Test ──────────────────────────
  - stage: DeployToTest
    displayName: 'Deploy Dev -> Test'
    dependsOn: Validate
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - job: DeployTest
        pool:
          vmImage: 'ubuntu-latest'
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '$(pythonVersion)'

          - script: pip install requests azure-identity
            displayName: 'Install dependencies'

          - task: AzureCLI@2
            displayName: 'Deploy to Test stage'
            inputs:
              azureSubscription: 'fabric-service-connection'
              scriptType: 'bash'
              scriptLocation: 'inlineScript'
              inlineScript: |
                python scripts/fabric_deploy.py \
                  --pipeline-id $(FABRIC_PIPELINE_ID) \
                  --source-stage 0 \
                  --note "Build $(Build.BuildNumber)"

  # ── Stage 3: Deploy Test to Prod (with approval) ────────
  - stage: DeployToProd
    displayName: 'Deploy Test -> Prod'
    dependsOn: DeployToTest
    jobs:
      - deployment: DeployProd
        environment: 'fabric-production'  # Requires approval in ADO
        pool:
          vmImage: 'ubuntu-latest'
        strategy:
          runOnce:
            deploy:
              steps:
                - task: UsePythonVersion@0
                  inputs:
                    versionSpec: '$(pythonVersion)'

                - script: pip install requests azure-identity
                  displayName: 'Install dependencies'

                - task: AzureCLI@2
                  displayName: 'Deploy to Production'
                  inputs:
                    azureSubscription: 'fabric-service-connection'
                    scriptType: 'bash'
                    scriptLocation: 'inlineScript'
                    inlineScript: |
                      python scripts/fabric_deploy.py \
                        --pipeline-id $(FABRIC_PIPELINE_ID) \
                        --source-stage 1 \
                        --note "Release $(Build.BuildNumber)"
```

## Deployment Script

```python
"""scripts/fabric_deploy.py -- Fabric deployment via REST API."""
import argparse, sys, time, requests
from azure.identity import DefaultAzureCredential

BASE_URL = "https://api.fabric.microsoft.com/v1"

def get_headers() -> dict:
    cred = DefaultAzureCredential()
    token = cred.get_token("https://api.fabric.microsoft.com/.default")
    return {"Authorization": f"Bearer {token.token}", "Content-Type": "application/json"}

def deploy(pipeline_id: str, source_stage: int, note: str) -> None:
    headers = get_headers()
    payload = {
        "sourceStageOrder": source_stage,
        "isBackwardDeployment": False,
        "newDeploymentNote": note,
    }
    resp = requests.post(
        f"{BASE_URL}/deploymentPipelines/{pipeline_id}/deploy",
        headers=headers, json=payload,
    )
    resp.raise_for_status()
    location = resp.headers.get("Location")
    if not location:
        return
    # Poll
    for _ in range(60):
        status_resp = requests.get(location, headers=headers)
        result = status_resp.json()
        status = result.get("status", "Unknown")
        print(f"Status: {status}")
        if status == "Succeeded":
            return
        if status in ("Failed", "Cancelled"):
            print(f"Deployment failed: {result}")
            sys.exit(1)
        time.sleep(10)
    sys.exit("Deployment timed out")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline-id", required=True)
    parser.add_argument("--source-stage", type=int, required=True)
    parser.add_argument("--note", default="Automated deployment")
    args = parser.parse_args()
    deploy(args.pipeline_id, args.source_stage, args.note)
```

## Service Principal Setup

| Step | Detail |
|------|--------|
| 1. Register app | Azure AD > App Registrations > New |
| 2. API permissions | Add `Fabric.ReadWrite.All` (delegated or app) |
| 3. Fabric admin | Grant SP access in Fabric Admin Portal |
| 4. ADO + workspace | Create ADO service connection; add SP as Admin on all stage workspaces |

## See Also

- [Deployment Pipelines](deployment-pipelines.md)
- [Git Integration](../concepts/git-integration.md)
- [Deployment Rules](../concepts/deployment-rules.md)
- [Environment Promotion](../concepts/environment-promotion.md)
