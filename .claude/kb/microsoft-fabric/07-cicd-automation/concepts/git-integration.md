> **MCP Validated:** 2026-03-26

# Git Integration

> **Purpose**: Connecting Fabric workspaces to Git repositories for version control and CI/CD
> **Confidence**: 0.95

## Overview

Fabric Git integration enables workspaces to connect to Azure DevOps or GitHub repositories, providing version control for Fabric items (lakehouses, warehouses, pipelines, notebooks, reports). Changes can be committed from the workspace to Git and synced back from Git to the workspace. This forms the foundation of CI/CD workflows in Fabric, supporting both Git-centric and hybrid deployment strategies.

**Key 2025-2026 updates:**
- **`fabric-cicd` Python library** (GA Feb 2026) -- official Microsoft tool for programmatic CI/CD deployments, replacing custom scripts
- **Azure DevOps cross-tenant support** (GA Nov 2025) -- Service Principal authentication across Azure AD tenants
- **Item-specific CI/CD tips** -- Microsoft's internal team published best practices for Notebooks, Pipelines, Lakehouses, and semantic models (Apr 2025)
- **Parameterization techniques** -- structured approach for environment-specific configs across Dev/Test/Prod

## The Pattern

```python
import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

def connect_workspace_to_git(workspace_id: str, headers: dict):
    """Connect a Fabric workspace to Azure DevOps Git repository."""
    git_config = {
        "gitProviderDetails": {
            "organizationName": "contoso",
            "projectName": "fabric-analytics",
            "gitProviderType": "AzureDevOps",
            "repositoryName": "fabric-items",
            "branchName": "main",
            "directoryName": "/workspace-dev"
        }
    }
    response = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/connect",
        headers=headers,
        json=git_config
    )
    return response.json()

def initialize_git_connection(workspace_id: str, headers: dict):
    """Initialize sync after connecting."""
    payload = {
        "initializationStrategy": "PreferWorkspace"
        # Options: PreferWorkspace, PreferRemote
    }
    response = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/initializeConnection",
        headers=headers,
        json=payload
    )
    return response.json()

def commit_to_git(workspace_id: str, headers: dict, message: str):
    """Commit workspace changes to Git."""
    payload = {
        "mode": "All",
        "comment": message
    }
    response = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/commitToGit",
        headers=headers,
        json=payload
    )
    return response.json()

def update_from_git(workspace_id: str, headers: dict):
    """Pull latest changes from Git to workspace."""
    payload = {
        "conflictResolutionPolicy": "PreferRemote"
        # Options: PreferRemote, PreferWorkspace
    }
    response = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/updateFromGit",
        headers=headers,
        json=payload
    )
    return response.json()
```

## Quick Reference

| Operation | API Endpoint | Method |
|-----------|-------------|--------|
| Connect to Git | `/git/connect` | POST |
| Initialize | `/git/initializeConnection` | POST |
| Get status | `/git/status` | GET |
| Commit to Git | `/git/commitToGit` | POST |
| Update from Git | `/git/updateFromGit` | POST |
| Disconnect | `/git/disconnect` | POST |

## Supported Item Types

| Item Type | Git Sync | Notes |
|-----------|----------|-------|
| Notebooks | Yes | .py or .ipynb format |
| Pipelines | Yes | JSON definition |
| Lakehouses | Yes | Metadata only (not data) |
| Warehouses | Yes | Metadata only |
| Semantic models | Yes | TMDL format |
| Reports | Yes | PBIR format |
| Environments | Yes | Configuration files |

## fabric-cicd Python Library (GA Feb 2026)

The official Microsoft CI/CD tool for Fabric, replacing custom deployment scripts:

```python
# pip install fabric-cicd
from fabric_cicd import FabricWorkspace, publish_all_items

# Connect to workspace
ws = FabricWorkspace(
    workspace_id="your-workspace-id",
    repository_directory="path/to/git/repo",
    item_type_in_scope=["Notebook", "DataPipeline", "Lakehouse", "SemanticModel"]
)

# Deploy all items from Git to workspace
publish_all_items(ws)
```

Key benefits:
- Handles item-specific deployment ordering (dependencies)
- Supports parameterization for environment-specific values
- Integrates with Azure DevOps and GitHub Actions pipelines

## Common Mistakes

### Wrong

```text
Connecting production workspace directly to 'main' branch for live editing
```

### Correct

```text
Git-centric approach:
  dev branch  --> dev workspace
  test branch --> test workspace
  main branch --> prod workspace (deploy via PR merge + fabric-cicd)
```

## Related

- [Deployment Pipelines](../patterns/deployment-pipelines.md)
- [REST API](../../05-apis-sdks/concepts/rest-api.md)
- [SDK Automation](../../05-apis-sdks/patterns/sdk-automation.md)
