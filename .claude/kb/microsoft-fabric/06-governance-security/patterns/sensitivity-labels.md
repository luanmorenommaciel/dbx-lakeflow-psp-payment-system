> **MCP Validated:** 2026-02-17

# Sensitivity Labels Across Workspaces

> **Purpose**: Implementing and managing sensitivity label policies across Fabric workspaces -- hierarchy, mandatory labeling, inheritance, and export restrictions

## When to Use

- Classifying Fabric items by sensitivity level (Public, Internal, Confidential, Highly Confidential)
- Enforcing mandatory labeling policies for regulated workspaces
- Controlling data export and sharing based on sensitivity classification
- Tracking label propagation from data sources through to reports

## Implementation

### Label Hierarchy

```text
Sensitivity Label Hierarchy (Microsoft Purview Information Protection)

  Public
    --> No restrictions on sharing or export
    --> Lowest sensitivity level

  General / Internal
    --> Internal use only
    --> External sharing blocked or warned

  Confidential
    --> Restricted sharing
    --> Export to unprotected formats blocked
    --> Encryption optional

  Highly Confidential
    --> No export allowed
    --> Mandatory encryption
    --> Watermarking on reports
    --> Access restricted to named users/groups
```

### Configuring Mandatory Labeling

Mandatory labeling ensures every Fabric item receives a sensitivity label:

```text
Admin Portal Configuration:
  Fabric Admin Portal
    --> Tenant settings
      --> Information protection
        --> Require users to apply sensitivity labels: ON
        --> Default label for new items: "General"

Purview Compliance Portal:
  compliance.microsoft.com
    --> Information protection
      --> Label policies
        --> Create policy
          --> Scope: Fabric items
          --> Require label: Yes
          --> Default label: General
          --> Users must justify label downgrade: Yes
```

### Label Inheritance Rules

```text
DOWNSTREAM INHERITANCE (automatic)
──────────────────────────────────
Source Lakehouse (Confidential)
  --> Derived Semantic Model: inherits "Confidential"
    --> Report built on model: inherits "Confidential"
      --> Dashboard tile: inherits "Confidential"

UPSTREAM INHERITANCE (manual trigger)
─────────────────────────────────────
Report labeled "Highly Confidential"
  --> Semantic model: NOT automatically upgraded
  --> Admin must manually relabel upstream items

CROSS-WORKSPACE INHERITANCE
────────────────────────────
Workspace A: Lakehouse (Confidential)
  --> Shortcut in Workspace B: inherits "Confidential"
    --> Items built on shortcut: inherit "Confidential"

CONFLICT RESOLUTION
───────────────────
When multiple sources have different labels:
  --> Highest sensitivity wins
  --> Example: Source A (Internal) + Source B (Confidential)
    --> Derived item: "Confidential"
```

### Applying Labels via Python

```python
"""Manage sensitivity labels across Fabric workspaces."""
import requests
from azure.identity import ClientSecretCredential

BASE_URL = "https://api.fabric.microsoft.com/v1"

def get_headers(credential) -> dict:
    token = credential.get_token("https://api.fabric.microsoft.com/.default")
    return {
        "Authorization": f"Bearer {token.token}",
        "Content-Type": "application/json",
    }

def apply_label_to_item(
    headers: dict, workspace_id: str, item_id: str, label_id: str,
) -> None:
    """Apply a sensitivity label to a single Fabric item."""
    resp = requests.post(
        f"{BASE_URL}/workspaces/{workspace_id}"
        f"/items/{item_id}/setSensitivityLabel",
        headers=headers,
        json={"sensitivityLabelId": label_id},
    )
    resp.raise_for_status()

def bulk_apply_labels(
    headers: dict, workspace_id: str, label_id: str,
    item_types: list[str] | None = None,
) -> int:
    """Apply a label to all items in a workspace, optionally filtered by type."""
    resp = requests.get(
        f"{BASE_URL}/workspaces/{workspace_id}/items",
        headers=headers,
    )
    resp.raise_for_status()
    items = resp.json()["value"]

    count = 0
    for item in items:
        if item_types and item["type"] not in item_types:
            continue
        try:
            apply_label_to_item(headers, workspace_id, item["id"], label_id)
            count += 1
        except requests.HTTPError as e:
            print(f"Failed to label {item['displayName']}: {e}")
    return count

# Usage
credential = ClientSecretCredential(
    tenant_id="tenant", client_id="client", client_secret="secret",
)
headers = get_headers(credential)

# Label all lakehouses and warehouses as Confidential
labeled = bulk_apply_labels(
    headers,
    workspace_id="ws-id",
    label_id="confidential-label-guid",
    item_types=["Lakehouse", "Warehouse"],
)
print(f"Applied labels to {labeled} items")
```

### Export Restrictions by Label

| Label | Export to PDF | Export to CSV | Share externally | Print |
|-------|-------------|---------------|-----------------|-------|
| Public | Allowed | Allowed | Allowed | Allowed |
| General | Allowed | Allowed | Warned | Allowed |
| Confidential | Allowed | Blocked | Blocked | Watermarked |
| Highly Confidential | Blocked | Blocked | Blocked | Blocked |

## Configuration

| Setting | Location | Description |
|---------|----------|-------------|
| Enable labels | Fabric Admin Portal | Tenant-wide toggle |
| Mandatory labeling | Purview Compliance Portal | Per-policy setting |
| Default label | Purview label policy | Applied to new items |
| Downgrade justification | Purview label policy | Require reason for downgrade |
| Inheritance | Automatic | Downstream propagation |

## Related

- [Purview Integration](../concepts/purview-integration.md)
- [Compliance Audit](compliance-audit.md)
- [RLS Security](../concepts/rls-security.md)
- [Dynamic Data Masking](data-masking.md)
