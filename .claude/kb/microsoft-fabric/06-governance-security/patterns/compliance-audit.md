> **MCP Validated:** 2026-02-17

# Compliance Auditing Pattern

> **Purpose**: Implementing compliance auditing in Fabric using audit logs, user activity tracking, and access reviews

## When to Use

- Tracking who accessed sensitive data and when
- Meeting regulatory compliance requirements (SOX, GDPR, HIPAA)
- Performing periodic access reviews for workspace role assignments
- Investigating security incidents with audit trail evidence
- Monitoring administrative actions across the Fabric tenant

## Implementation

### Unified Audit Log Queries (KQL)

```kql
// Query Fabric audit events in Microsoft 365 Unified Audit Log
// Access via: compliance.microsoft.com --> Audit

// 1. All Fabric activities in the last 7 days
AuditLogs
| where TimeGenerated > ago(7d)
| where RecordType == "PowerBIAudit"
| project TimeGenerated, UserId, Operation, ItemName, WorkspaceName
| order by TimeGenerated desc
| take 100

// 2. Data access events -- who queried what
AuditLogs
| where TimeGenerated > ago(30d)
| where Operation in ("GetModels", "ExecuteQueries", "QueryDatasetData")
| summarize AccessCount = count() by UserId, ItemName, WorkspaceName
| order by AccessCount desc

// 3. Sensitive data access -- track labeled item interactions
AuditLogs
| where TimeGenerated > ago(30d)
| where Operation in ("ViewReport", "ExportReport", "GetDatasetData")
| where SensitivityLabelId != ""
| project TimeGenerated, UserId, Operation, ItemName, SensitivityLabelId
| order by TimeGenerated desc

// 4. Administrative actions -- role changes, workspace modifications
AuditLogs
| where TimeGenerated > ago(30d)
| where Operation in (
    "AddWorkspaceMembers", "DeleteWorkspaceMembers",
    "UpdateWorkspaceAccess", "CreateWorkspace", "DeleteWorkspace",
    "SetScheduledRefresh", "UpdateDatasources"
)
| project TimeGenerated, UserId, Operation, ItemName, WorkspaceName
| order by TimeGenerated desc

// 5. Export and download tracking
AuditLogs
| where TimeGenerated > ago(30d)
| where Operation in (
    "ExportReport", "ExportDataflow", "DownloadReport",
    "ExportArtifact", "ExportTile"
)
| summarize ExportCount = count() by UserId, Operation, ItemName
| order by ExportCount desc
```

### Activity Events via REST API

```python
"""Query Fabric activity events programmatically."""
import requests
from datetime import datetime, timedelta
from azure.identity import ClientSecretCredential

def get_activity_events(
    credential, start_date: str, end_date: str,
    activity_filter: str | None = None,
) -> list[dict]:
    """
    Fetch Fabric/Power BI activity events.
    Dates in format: 'YYYY-MM-DDT00:00:00.000Z'
    """
    token = credential.get_token(
        "https://analysis.windows.net/powerbi/api/.default"
    )
    headers = {"Authorization": f"Bearer {token.token}"}

    url = (
        f"https://api.powerbi.com/v1.0/myorg/admin/activityevents"
        f"?startDateTime='{start_date}'&endDateTime='{end_date}'"
    )
    if activity_filter:
        url += f"&$filter=Activity eq '{activity_filter}'"

    all_events = []
    while url:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        all_events.extend(data.get("activityEventEntities", []))
        url = data.get("continuationUri")
    return all_events

# Usage: Get all export events from the last 7 days
credential = ClientSecretCredential(
    tenant_id="your-tenant", client_id="your-client", client_secret="your-secret",
)
end = datetime.utcnow()
start = end - timedelta(days=7)
events = get_activity_events(
    credential,
    start_date=start.strftime("%Y-%m-%dT00:00:00.000Z"),
    end_date=end.strftime("%Y-%m-%dT00:00:00.000Z"),
    activity_filter="ExportReport",
)
print(f"Found {len(events)} export events")
```

### Access Review Automation

```python
"""Automated workspace access review -- flag stale permissions."""
import requests
from datetime import datetime, timedelta

def review_workspace_access(
    credential, workspace_id: str, stale_days: int = 90,
) -> list[dict]:
    """Identify workspace members who haven't accessed in N days."""
    token = credential.get_token(
        "https://api.fabric.microsoft.com/.default"
    )
    headers = {"Authorization": f"Bearer {token.token}"}

    # Get current workspace members
    resp = requests.get(
        f"https://api.fabric.microsoft.com/v1"
        f"/workspaces/{workspace_id}/roleAssignments",
        headers=headers,
    )
    resp.raise_for_status()
    members = resp.json()["value"]

    # Get activity events for the review period
    cutoff = datetime.utcnow() - timedelta(days=stale_days)
    end = datetime.utcnow()
    events = get_activity_events(
        credential,
        start_date=cutoff.strftime("%Y-%m-%dT00:00:00.000Z"),
        end_date=end.strftime("%Y-%m-%dT00:00:00.000Z"),
    )

    active_users = {e["UserId"] for e in events if "UserId" in e}

    stale_members = []
    for member in members:
        principal = member.get("principal", {})
        user_id = principal.get("id", "")
        if user_id not in active_users:
            stale_members.append({
                "principal_id": user_id,
                "display_name": principal.get("displayName", "Unknown"),
                "role": member.get("role", "Unknown"),
                "days_inactive": stale_days,
            })
    return stale_members
```

## Key Audit Events

| Event | Operation Name | Category |
|-------|---------------|----------|
| View report | `ViewReport` | Data access |
| Query dataset | `ExecuteQueries` | Data access |
| Export report | `ExportReport` | Data movement |
| Refresh dataset | `RefreshDataset` | Data processing |
| Add workspace member | `AddWorkspaceMembers` | Administration |
| Delete workspace | `DeleteWorkspace` | Administration |
| Change sensitivity label | `SensitivityLabelApplied` | Governance |
| Share item | `ShareReport` | Collaboration |

## Configuration

| Setting | Value | Description |
|---------|-------|-------------|
| Audit log retention | 90 days (default) | Extended to 1 year with E5 |
| Activity API lookback | 30 days max per call | Use pagination for full history |
| Event latency | 5-30 minutes | Events are near-real-time |
| Export limit | 10,000 events/query | Use continuationUri for more |

## Related

- [Sensitivity Labels](sensitivity-labels.md)
- [Purview Integration](../concepts/purview-integration.md)
- [RLS Security](../concepts/rls-security.md)
- [Dynamic Data Masking](data-masking.md)
