> **MCP Validated:** 2026-02-17

# Git Branching Strategies with Fabric

> **Purpose**: Git branch workflows for Fabric workspaces -- feature branches, workspace-branch mapping, conflict resolution, and selective sync

## When to Use

- Teams collaborating on shared Fabric items (notebooks, pipelines, semantic models)
- Managing parallel feature development with workspace Git integration
- Resolving conflicts when multiple developers modify the same Fabric item
- Implementing selective sync for controlled workspace updates

## Overview

Fabric Git integration maps a workspace to a single Git branch and directory. Branching strategies determine how developers collaborate, isolate changes, and promote through environments. The two primary approaches are feature-branch workflow (branch per feature, merge via PR) and environment-branch workflow (branch per stage, promote via merge). Fabric supports Azure DevOps and GitHub repositories.

## Branch Workflow Diagram

```text
                    ┌─────────────────────────────────────────────────┐
                    │                 Git Repository                   │
                    └─────────────────────────────────────────────────┘
                         │              │              │
                      main           test            dev
                         │              │              │
                         ▼              ▼              ▼
                    ┌─────────┐   ┌─────────┐   ┌─────────┐
                    │ WS:Prod │   │ WS:Test │   │ WS:Dev  │
                    └─────────┘   └─────────┘   └─────────┘
                                                     │
                                       ┌─────────────┼─────────────┐
                                       │             │             │
                                  feature/a     feature/b    feature/c
                                       │             │             │
                                       ▼             ▼             ▼
                                  ┌────────┐   ┌────────┐   ┌────────┐
                                  │WS:Feat │   │WS:Feat │   │WS:Feat │
                                  │   A    │   │   B    │   │   C    │
                                  └────────┘   └────────┘   └────────┘

    Promotion flow:
    feature/* ──PR──▶ dev ──PR──▶ test ──PR──▶ main
                       │           │            │
                   auto-sync   auto-sync   auto-sync
                       ▼           ▼            ▼
                    WS:Dev      WS:Test     WS:Prod
```

## Implementation

### Workspace-Branch Mapping

```python
import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

def connect_feature_branch(
    workspace_id: str, branch_name: str, headers: dict,
) -> dict:
    """Connect a workspace to a feature branch."""
    git_config = {
        "gitProviderDetails": {
            "organizationName": "contoso",
            "projectName": "fabric-analytics",
            "gitProviderType": "AzureDevOps",
            "repositoryName": "fabric-items",
            "branchName": branch_name,
            "directoryName": f"/workspaces/{branch_name}",
        }
    }
    resp = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/connect",
        headers=headers, json=git_config,
    )
    resp.raise_for_status()
    return resp.json()


def switch_workspace_branch(
    workspace_id: str, new_branch: str, headers: dict,
) -> dict:
    """Switch a workspace to a different branch."""
    # Disconnect current branch
    requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/disconnect",
        headers=headers,
    )
    # Reconnect to new branch
    return connect_feature_branch(workspace_id, new_branch, headers)
```

### Conflict Resolution

```python
def resolve_conflicts_and_sync(
    workspace_id: str, headers: dict,
    strategy: str = "PreferRemote",
) -> dict:
    """
    Resolve conflicts during Git sync.

    Strategies:
      PreferRemote  - Git branch wins (safe default for shared branches)
      PreferWorkspace - Workspace wins (use for personal feature branches)
    """
    payload = {
        "conflictResolutionPolicy": strategy,
        "allowOverrideItems": True,
    }
    resp = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/updateFromGit",
        headers=headers, json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

### Selective Sync

```python
def commit_specific_items(
    workspace_id: str, headers: dict,
    item_ids: list[str], message: str,
) -> dict:
    """Commit only specific items to Git (selective sync)."""
    payload = {
        "mode": "Selective",
        "items": [{"objectId": item_id} for item_id in item_ids],
        "comment": message,
    }
    resp = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}/git/commitToGit",
        headers=headers, json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

## Strategy Comparison

| Strategy | Isolation | Complexity | Best For |
|----------|-----------|------------|----------|
| Feature branches | High | Medium | Multiple developers, parallel work |
| Environment branches only | Low | Low | Small teams, sequential changes |
| Trunk-based (dev only) | None | Lowest | Solo developer or prototyping |
| Gitflow + Fabric | High | High | Enterprise with release cycles |

## Conflict Scenarios

| Scenario | Resolution | Risk |
|----------|-----------|------|
| Same notebook, different cells | Auto-merge (JSON) | Low |
| Same notebook, same cell | Manual or policy-based | Medium |
| Semantic model schema conflict | PreferRemote recommended | Medium |
| Pipeline definition conflict | Review diff in Git | High |
| Lakehouse metadata conflict | PreferRemote (data unaffected) | Low |

## Common Mistakes

### Wrong

```text
Multiple developers editing the same workspace without branching,
then committing over each other's changes.
```

### Correct

```text
1. Each developer creates feature/xyz branch from dev
2. Connect personal workspace to feature branch
3. Develop and commit to feature branch
4. Open PR from feature/* -> dev
5. After merge, dev workspace auto-syncs
```

## See Also

- [Git Integration](../concepts/git-integration.md)
- [Environment Promotion](../concepts/environment-promotion.md)
- [Deployment Pipelines](deployment-pipelines.md)
- [Azure DevOps Pipeline](azure-devops-pipeline.md)
