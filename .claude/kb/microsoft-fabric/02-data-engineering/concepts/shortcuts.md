> **MCP Validated:** 2026-02-17

# Shortcuts

> **Purpose**: OneLake shortcuts for cross-cloud and cross-workspace data access without data movement
> **Confidence**: 0.95

## Overview

OneLake shortcuts are zero-copy, read-only references to data stored outside the current Lakehouse. They appear as virtual folders but do not duplicate data -- queries resolve at the source. Shortcuts enable federated access across ADLS Gen2, Amazon S3, Google Cloud Storage (GCS), Dataverse, and other Fabric workspaces without data movement costs.

## Shortcut Types

| Type | Source | Authentication |
|------|--------|---------------|
| **OneLake** | Another Fabric Lakehouse/Warehouse | Fabric identity |
| **ADLS Gen2** | Azure Data Lake Storage | Service principal or SAS |
| **Amazon S3** | AWS S3 buckets | IAM role or access key |
| **Google Cloud Storage** | GCS buckets | Service account key |
| **Dataverse** | Power Platform / Dynamics 365 | Entra ID (managed) |
| **S3 Compatible** | MinIO, Cloudflare R2, etc. | Access key |

## Architecture

```text
Lakehouse (Consumer)
├── Tables/
│   ├── local_table (Delta, in OneLake)
│   └── shared_customers ── Shortcut ──▶ Lakehouse B/Tables/customers
├── Files/
│   ├── external_data/ ──── Shortcut ──▶ ADLS Gen2/container/path/
│   └── aws_logs/ ───────── Shortcut ──▶ S3://bucket/prefix/
```

## Creating Shortcuts via REST API

```python
import requests

def create_onelake_shortcut(
    workspace_id: str, lakehouse_id: str, shortcut_name: str,
    target_workspace_id: str, target_lakehouse_id: str,
    target_path: str, headers: dict,
):
    """Create a shortcut to another OneLake Lakehouse."""
    endpoint = (
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
        f"/items/{lakehouse_id}/shortcuts"
    )
    payload = {
        "path": f"Tables/{shortcut_name}",
        "name": shortcut_name,
        "target": {
            "oneLake": {
                "workspaceId": target_workspace_id,
                "itemId": target_lakehouse_id,
                "path": target_path,
            }
        },
    }
    return requests.post(endpoint, headers=headers, json=payload).json()
```

## Security Inheritance

| Aspect | Behavior |
|--------|----------|
| **OneLake shortcuts** | Inherits permissions from source Lakehouse |
| **External shortcuts** | Uses the connection's stored credentials |
| **SQL endpoint** | Visible if user has Lakehouse read access |
| **Row-level security** | Not enforced through shortcuts (apply at source) |

## Cross-Workspace Patterns

```text
Pattern 1: Shared Reference Data
  Central Lakehouse --> dim_customers, dim_products
  Team A shortcut --> dim_customers (single source of truth)
  Team B shortcut --> dim_customers

Pattern 2: Multi-Cloud Federation
  Lakehouse Tables (OneLake) + Files/aws_logs (S3) + Files/gcp_archive (GCS)
  Spark can JOIN across all three in a single query.
```

## Limitations

| Limitation | Details |
|------------|---------|
| Read-only | Cannot write through a shortcut |
| No nesting | A shortcut cannot point to another shortcut |
| Table shortcuts | Must point to Delta format for SQL endpoint |
| File shortcuts | Any format, but not queryable via SQL endpoint |

## Common Mistakes

### Wrong

```text
Creating a shortcut to raw CSV in S3 under Tables/, expecting SQL access
--> SQL endpoint only works with Delta tables in Tables/
```

### Correct

```text
Create file shortcut under Files/ and use Spark to read CSV,
OR convert source to Delta and create a Table shortcut
```

## Related

- [Lakehouse](lakehouse.md)
- [Spark Notebooks](spark-notebooks.md)
- [Dataflow Gen2](dataflow-gen2.md)
- [Copy Activity](../patterns/copy-activity.md)
