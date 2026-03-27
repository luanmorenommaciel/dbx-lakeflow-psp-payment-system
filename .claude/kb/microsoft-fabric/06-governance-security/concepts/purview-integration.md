> **MCP Validated:** 2026-02-17

# Microsoft Purview Integration

> **Purpose**: Integrating Microsoft Purview with Fabric for sensitivity labels, data classification, lineage tracking, and compliance policies
> **Confidence**: 0.90

## Overview

Microsoft Purview integrates natively with Microsoft Fabric to provide unified data governance across the entire analytics platform. Key capabilities include automatic sensitivity label propagation across Fabric items, data classification scanning for PII and sensitive content, end-to-end data lineage from source to report, and compliance policy enforcement. Purview treats Fabric as a first-class data source with bi-directional metadata exchange.

## Sensitivity Labels

Sensitivity labels from Microsoft Purview Information Protection apply across Fabric items:

| Fabric Item | Label Support | Inheritance |
|-------------|---------------|-------------|
| Lakehouse | Yes | Propagates to downstream items |
| Warehouse | Yes | Propagates to downstream items |
| Semantic model | Yes | Inherits from data source |
| Report | Yes | Inherits from semantic model |
| Dataflow Gen2 | Yes | Inherits from source |
| Pipeline | Metadata only | No content labeling |
| Notebook | Yes | Manual or inherited |

### Label Propagation Flow

```text
Lakehouse (Confidential)
  --> Semantic Model (inherits: Confidential)
    --> Report (inherits: Confidential)
      --> Export (restricted by label policy)
```

### Applying Labels via API

```python
import requests

def apply_sensitivity_label(
    headers: dict, workspace_id: str, item_id: str, label_id: str,
) -> None:
    """Apply a Purview sensitivity label to a Fabric item."""
    resp = requests.post(
        f"https://api.fabric.microsoft.com/v1"
        f"/workspaces/{workspace_id}/items/{item_id}/setSensitivityLabel",
        headers=headers,
        json={"sensitivityLabelId": label_id},
    )
    resp.raise_for_status()
```

## Data Classification

Purview scans Fabric data assets for sensitive data types:

| Classification | Examples | Auto-Detection |
|----------------|----------|----------------|
| Personal | Name, address, phone | Yes |
| Financial | Credit card, bank account | Yes |
| Health | Medical ID, diagnosis codes | Yes |
| Government | SSN, passport, driver license | Yes |
| Custom | Employee ID, project codes | Configurable |

### Scanning Configuration

```text
Purview Data Map
  --> Register Fabric as data source
    --> Configure scan rule set
      --> Schedule full/incremental scans
        --> Review classifications in Data Catalog
```

## Data Lineage

Purview captures end-to-end lineage across Fabric:

```text
External Source (SQL Server, S3, API)
  --> Data Pipeline / Dataflow Gen2
    --> Lakehouse (Bronze)
      --> Notebook (transformation)
        --> Lakehouse (Silver/Gold)
          --> Semantic Model
            --> Power BI Report
```

### Lineage Capabilities

| Feature | Description |
|---------|-------------|
| Column-level lineage | Track transformations per column |
| Cross-workspace | Lineage spans workspace boundaries |
| External sources | Includes on-premises and cloud sources |
| Impact analysis | Identify downstream consumers of a dataset |
| Refresh lineage | Track data freshness and refresh times |

## Compliance Policies

| Policy Type | Scope | Enforcement |
|-------------|-------|-------------|
| Data Loss Prevention (DLP) | Semantic models, reports | Block export of labeled content |
| Retention | Fabric items | Auto-delete after period |
| Sensitivity label | All items | Mandatory labeling per workspace |
| Access reviews | Workspace roles | Periodic recertification |
| Audit logging | All operations | Unified audit log |

## Admin Portal Configuration

```text
Fabric Admin Portal --> Governance and insights
  --> Information protection
    --> Enable sensitivity labels: ON
    --> Apply labels automatically: ON
    --> Mandatory label policy: Per workspace setting
    --> Export restrictions: Enforce label-based restrictions
```

## Quick Reference

| Integration Point | Configuration | Purpose |
|-------------------|---------------|---------|
| Sensitivity labels | Admin portal + Purview | Classify and protect items |
| Data classification | Purview Data Map scan | Auto-detect PII |
| Lineage | Automatic capture | End-to-end tracking |
| DLP policies | Purview compliance portal | Prevent data leaks |
| Audit logs | Microsoft 365 audit | Activity tracking |

## Related

- [Sensitivity Labels Pattern](../patterns/sensitivity-labels.md)
- [Compliance Audit Pattern](../patterns/compliance-audit.md)
- [RLS Security](rls-security.md)
- [CLS Security](cls-security.md)
