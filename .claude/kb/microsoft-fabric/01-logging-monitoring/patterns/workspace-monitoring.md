> **MCP Validated:** 2026-02-17

# Workspace Monitoring

> **Purpose**: Patterns for enabling and querying Fabric workspace diagnostics with KQL

## When to Use

- Troubleshooting pipeline failures and slow queries across a Fabric workspace
- Building operational dashboards for capacity utilization and job health
- Auditing user activity and query performance over time
- Detecting anomalies in data refresh patterns or resource consumption

## Implementation

```kql
// Query 1: Pipeline job failures in the last 24 hours
ItemJobEventLogs
| where Timestamp > ago(24h)
| where Status == "Failed"
| project Timestamp, ItemName, ItemType, Status, ErrorMessage, DurationMs
| order by Timestamp desc

// Query 2: Top 10 slowest queries by duration
EventhouseQueryLogs
| where Timestamp > ago(7d)
| where DurationMs > 0
| top 10 by DurationMs desc
| project Timestamp, User, QueryText, DurationMs, CpuTimeMs, RowCount

// Query 3: Hourly job success/failure trend
ItemJobEventLogs
| where Timestamp > ago(7d)
| summarize
    SuccessCount = countif(Status == "Completed"),
    FailureCount = countif(Status == "Failed"),
    AvgDurationMs = avg(DurationMs)
  by bin(Timestamp, 1h)
| render timechart

// Query 4: Capacity consumption by item type
ItemJobEventLogs
| where Timestamp > ago(24h)
| summarize
    TotalCpuMs = sum(CpuTimeMs),
    JobCount = count()
  by ItemType
| order by TotalCpuMs desc
| render piechart

// Query 5: Semantic model refresh monitoring
SemanticModelLogs
| where Timestamp > ago(24h)
| where OperationName == "CommandEnd"
| extend DurationSec = DurationMs / 1000.0
| project Timestamp, ItemName, OperationName, DurationSec, User
| order by DurationSec desc

// Query 6: Error pattern detection
ItemJobEventLogs
| where Timestamp > ago(7d)
| where Status == "Failed"
| extend ErrorCategory = case(
    ErrorMessage has "timeout", "Timeout",
    ErrorMessage has "memory", "OutOfMemory",
    ErrorMessage has "throttl", "Throttled",
    ErrorMessage has "auth", "Authentication",
    "Other"
  )
| summarize ErrorCount = count() by ErrorCategory, ItemType
| order by ErrorCount desc
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Monitoring Eventhouse | Auto-created | Read-only KQL database per workspace |
| Monitoring Eventstream | Auto-created | Streams diagnostics to Eventhouse |
| Retention period | 30 days | Default log retention |
| Enable monitoring | Admin toggle | Workspace settings > Monitoring |
| Dashboard templates | GitHub repo | `microsoft/fabric-toolbox` |

## Setup Steps

```text
1. Open Workspace Settings > Monitoring
2. Toggle "Enable workspace monitoring" to ON
3. Wait 5-10 minutes for Eventhouse + Eventstream creation
4. Open Monitoring Eventhouse > KQL Queryset
5. Query tables: ItemJobEventLogs, SemanticModelLogs, EventhouseQueryLogs
```

## Example Usage

```python
# Python: Query workspace monitoring via Fabric REST API
import requests

def get_monitoring_data(workspace_id: str, headers: dict, kql_query: str):
    """Execute a KQL query against the monitoring Eventhouse."""
    # Get the monitoring database endpoint
    endpoint = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/kqlDatabases"
    databases = requests.get(endpoint, headers=headers).json()["value"]

    monitoring_db = next(
        (db for db in databases if "monitoring" in db["displayName"].lower()),
        None
    )
    if not monitoring_db:
        raise ValueError("Monitoring Eventhouse not found. Enable workspace monitoring first.")

    # Execute KQL query
    query_endpoint = f"{endpoint}/{monitoring_db['id']}/executeQuery"
    payload = {"query": kql_query}
    result = requests.post(query_endpoint, headers=headers, json=payload)
    return result.json()

# Get failed jobs from last 24h
failures = get_monitoring_data(
    workspace_id="your-workspace-id",
    headers=headers,
    kql_query="ItemJobEventLogs | where Timestamp > ago(24h) | where Status == 'Failed'"
)
```

## Available Monitoring Tables

| Table | Contents | Key Columns |
|-------|----------|-------------|
| `ItemJobEventLogs` | Pipeline and job events | ItemName, Status, DurationMs, ErrorMessage |
| `SemanticModelLogs` | Power BI model traces | OperationName, DurationMs, CpuTimeMs |
| `EventhouseQueryLogs` | KQL query execution | QueryText, DurationMs, RowCount |
| `EventhouseCommandLogs` | KQL management commands | CommandText, ResourceUtilization |

## See Also

- [KQL Queries](../concepts/kql-queries.md)
- [REST API](../../05-apis-sdks/concepts/rest-api.md)
