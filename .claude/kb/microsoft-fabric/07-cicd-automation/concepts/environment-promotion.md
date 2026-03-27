> **MCP Validated:** 2026-02-17

# Environment Promotion

> **Purpose**: Strategies for promoting Fabric items across Dev, Test, and Prod environments
> **Confidence**: 0.95

## Overview

Environment promotion in Fabric moves workspace items through lifecycle stages (Dev, Test, Prod). Two primary strategies exist: workspace-per-stage (each environment is a separate workspace bound to a deployment pipeline stage) and branch-per-stage (each environment maps to a Git branch with workspace sync). Item configuration overrides -- data source rules and parameter rules -- ensure environment-specific connections and settings are applied automatically during promotion.

## Promotion Strategies

### Workspace-per-Stage

```text
┌──────────────┐    Deploy     ┌──────────────┐    Deploy     ┌──────────────┐
│  WS: Dev     │──────────────▶│  WS: Test    │──────────────▶│  WS: Prod    │
│  Capacity: F2│   (auto)      │  Capacity: F8│   (approval)  │  Capacity:F64│
│  Branch: dev │               │  Branch: test│               │  Branch: main│
└──────────────┘               └──────────────┘               └──────────────┘
       │                              │                              │
       └──── Deployment Pipeline ─────┴──── Deployment Pipeline ─────┘
```

| Aspect | Detail |
|--------|--------|
| Setup | 1 workspace per stage, bound to pipeline |
| Git mapping | Optional: branch per workspace |
| Config overrides | Data source + parameter rules on pipeline |
| Best for | Teams wanting UI-driven promotion |

### Branch-per-Stage

```text
┌──────────────┐    PR/Merge   ┌──────────────┐    PR/Merge   ┌──────────────┐
│ Branch: dev  │──────────────▶│ Branch: test │──────────────▶│ Branch: main │
│   ↕ sync     │               │   ↕ sync     │               │   ↕ sync     │
│ WS: Dev      │               │ WS: Test     │               │ WS: Prod     │
└──────────────┘               └──────────────┘               └──────────────┘
```

| Aspect | Detail |
|--------|--------|
| Setup | Branch-to-workspace mapping via Git integration |
| Promotion | Git PR merge triggers workspace sync |
| Config overrides | Managed in Git (config files) or deployment rules |
| Best for | Teams with strong Git workflows |

## Item Configuration Overrides

### Data Source Rules

```python
import requests

BASE_URL = "https://api.fabric.microsoft.com/v1"

def configure_data_source_override(
    pipeline_id: str, stage_order: int, headers: dict,
) -> dict:
    """Override data source connections when deploying to a stage."""
    payload = {
        "rules": [{
            "itemType": "Lakehouse",
            "itemDisplayName": "analytics_lakehouse",
            "dataSourceRules": [{
                "sourceConnection": "dev-workspace-onelake.dfs.fabric.microsoft.com",
                "targetConnection": "prod-workspace-onelake.dfs.fabric.microsoft.com",
            }],
        }]
    }
    resp = requests.post(
        f"{BASE_URL}/deploymentPipelines/{pipeline_id}"
        f"/stages/{stage_order}/deploymentRules",
        headers=headers, json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

### Parameter Rules

```python
def configure_parameter_override(
    pipeline_id: str, stage_order: int, headers: dict,
) -> dict:
    """Override semantic model parameters per environment."""
    payload = {
        "rules": [{
            "itemType": "SemanticModel",
            "itemDisplayName": "sales_report_model",
            "parameterRules": [
                {"parameterName": "Environment", "targetValue": "Production"},
                {"parameterName": "RefreshSchedule", "targetValue": "Hourly"},
            ],
        }]
    }
    resp = requests.post(
        f"{BASE_URL}/deploymentPipelines/{pipeline_id}"
        f"/stages/{stage_order}/deploymentRules",
        headers=headers, json=payload,
    )
    resp.raise_for_status()
    return resp.json()
```

## Strategy Comparison

| Factor | Workspace-per-Stage | Branch-per-Stage |
|--------|-------------------|-----------------|
| Complexity | Lower | Higher |
| Version control | Optional | Required |
| Rollback | Backward deploy | Git revert + sync |
| Audit trail | Pipeline history | Git commit log |
| Config overrides | Deployment rules | Git config + rules |
| Approval gates | Built-in pipeline | PR reviews + pipeline |

## Common Mistakes

### Wrong

```text
Using the same workspace for all environments with manual config changes
```

### Correct

```text
Separate workspace per stage with automated config overrides:
  Dev:  dev connections, F2 capacity, auto-deploy enabled
  Test: test connections, F8 capacity, smoke tests required
  Prod: prod connections, F64 capacity, approval gate enforced
```

## Related

- [Deployment Rules](deployment-rules.md)
- [Git Integration](git-integration.md)
- [Deployment Pipelines](../patterns/deployment-pipelines.md)
- [Git Branching](../patterns/git-branching.md)
