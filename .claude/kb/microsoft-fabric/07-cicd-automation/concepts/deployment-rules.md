> **MCP Validated:** 2026-02-17

# Deployment Rules

> **Purpose**: Configuring deployment pipeline rules for approval gates, auto-deployment, cross-stage constraints, and item-level overrides
> **Confidence**: 0.95

## Overview

Fabric deployment pipeline rules govern how items move between stages. Rules can enforce approval gates before production, enable automatic deployment on commit, bind capacity per stage, and apply item-level overrides for data source connections and parameters. Rules are configured per pipeline stage and can be managed through the Fabric UI or REST API.

## Rule Types

| Rule Type | Scope | Description |
|-----------|-------|-------------|
| Approval gates | Stage | Require designated approvers before deployment proceeds |
| Auto-deployment | Stage | Automatically deploy when source stage changes |
| Data source rules | Item | Override connection strings per stage |
| Parameter rules | Item | Override parameter values per stage |
| Capacity binding | Stage | Assign specific Fabric capacity to each stage |
| Item-level rules | Item | Include/exclude specific items from deployment |

## Approval Gates

```python
import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

def configure_approval_gate(
    pipeline_id: str, stage_order: int, headers: dict,
    approvers: list[str],
) -> dict:
    """Set approval requirements for a deployment stage."""
    payload = {
        "stageOrder": stage_order,
        "rules": {
            "approvalRequired": True,
            "approvers": [
                {"principalId": uid, "principalType": "User"}
                for uid in approvers
            ],
            "minimumApprovals": 1,
            "requestorCanApprove": False,
        },
    }
    resp = requests.patch(
        f"{BASE_URL}/deploymentPipelines/{pipeline_id}/stages/{stage_order}",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

## Auto-Deployment Rules

```python
def enable_auto_deployment(
    pipeline_id: str, source_stage: int, headers: dict,
) -> dict:
    """Enable automatic deployment when source stage is updated."""
    payload = {
        "autoDeployment": {
            "enabled": True,
            "deployOnCommit": True,  # Deploy when Git commit lands
            "deployOnManual": False, # Skip manual trigger events
        }
    }
    resp = requests.patch(
        f"{BASE_URL}/deploymentPipelines/{pipeline_id}"
        f"/stages/{source_stage}/autoDeployment",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

## Data Source and Parameter Rules

```python
def set_deployment_rules(
    pipeline_id: str, stage_order: int, headers: dict,
) -> dict:
    """Configure data source overrides for a stage."""
    payload = {
        "rules": [
            {
                "itemType": "Lakehouse",
                "itemDisplayName": "sales_lakehouse",
                "dataSourceRules": [{
                    "sourceConnection": "dev-server.database.fabric.microsoft.com",
                    "targetConnection": "prod-server.database.fabric.microsoft.com",
                }],
            },
            {
                "itemType": "SemanticModel",
                "itemDisplayName": "sales_model",
                "parameterRules": [{
                    "parameterName": "Environment",
                    "targetValue": "Production",
                }],
            },
        ]
    }
    resp = requests.post(
        f"{BASE_URL}/deploymentPipelines/{pipeline_id}"
        f"/stages/{stage_order}/deploymentRules",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

## Cross-Stage Rule Matrix

| Rule | Dev -> Test | Test -> Prod | Notes |
|------|------------|--------------|-------|
| Approval | Optional | Required | At least 1 non-requestor |
| Auto-deploy | Recommended | Disabled | Prod always manual/gated |
| Data source swap | Dev -> Test conn | Test -> Prod conn | Per item type |
| Parameter override | `env=test` | `env=prod` | Cascading values |
| Capacity binding | Dev F2 | Prod F64 | Match workload sizing |

## Common Mistakes

### Wrong

```text
Enabling auto-deployment directly to production without approval gates
```

### Correct

```text
Auto-deploy: Dev -> Test (automated)
Approval gate: Test -> Prod (requires sign-off)
Data source rules: override connections at each stage
```

## Related

- [Deployment Pipelines](../patterns/deployment-pipelines.md)
- [Environment Promotion](environment-promotion.md)
- [Git Integration](git-integration.md)
