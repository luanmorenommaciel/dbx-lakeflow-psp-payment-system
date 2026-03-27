> **MCP Validated:** 2026-02-17

# Error Tracking

> **Purpose**: Pattern for centralized error tracking across Fabric workloads with KQL-based aggregation and root cause correlation

## When to Use

- Centralizing error visibility across pipelines, notebooks, dataflows, and Spark jobs
- Calculating failure rates and error budgets for SLA tracking
- Correlating errors across pipeline stages to identify root causes
- Building automated triage workflows with error categorization

## Implementation

### Step 1: Unified Error Table

```kql
.create table ErrorEvents (
    Timestamp: datetime, WorkloadType: string, ItemName: string,
    ErrorCategory: string, ErrorCode: string, ErrorMessage: string,
    Severity: string, CorrelationId: guid, DurationMs: long, RetryCount: int
)

.alter-merge table ErrorEvents policy retention
```
```json
{ "SoftDeletePeriod": "180.00:00:00", "Recoverability": "Enabled" }
```

### Step 2: Error Categorization Function

```kql
.create-or-alter function CategorizeError(msg: string) {
    case(
        msg has_any ("timeout", "timed out"), "Timeout",
        msg has_any ("out of memory", "OOM"), "OutOfMemory",
        msg has_any ("throttl", "rate limit", "429"), "Throttled",
        msg has_any ("auth", "401", "403"), "Authentication",
        msg has_any ("not found", "404"), "NotFound",
        msg has_any ("connection", "network"), "Network",
        msg has_any ("schema", "type mismatch"), "SchemaError",
        msg has_any ("capacity", "CU"), "CapacityLimit",
        "Uncategorized"
    )
}
```

### Step 3: Error Aggregation Queries

```kql
// Error summary by category (last 24 hours)
ErrorEvents
| where Timestamp > ago(24h)
| extend Category = CategorizeError(ErrorMessage)
| summarize ErrorCount = count(), UniqueItems = dcount(ItemName),
    LastOccurrence = max(Timestamp)
  by Category, WorkloadType
| order by ErrorCount desc

// Failure rate per pipeline (last 7 days, hourly)
PipelineEvents
| where Timestamp > ago(7d)
| summarize Total = count(), Failed = countif(Status == "Failed")
  by PipelineName, bin(Timestamp, 1h)
| extend FailureRatePct = round(100.0 * Failed / Total, 2)

// Error budget tracking (99.5% SLA target)
let sla_target = 99.5;
PipelineEvents
| where Timestamp > ago(30d)
| summarize Total = count(), Success = countif(Status == "Completed")
| extend SuccessRatePct = round(100.0 * Success / Total, 3),
    ErrorBudgetTotal = round(Total * (1.0 - sla_target / 100.0), 0),
    ErrorBudgetUsed = Total - Success
| extend BudgetRemaining = ErrorBudgetTotal - ErrorBudgetUsed

// Root cause correlation (errors that co-occur across stages)
ErrorEvents
| where Timestamp > ago(24h) and isnotempty(CorrelationId)
| summarize
    ErrorChain = make_list(pack("item", ItemName, "category", ErrorCategory)),
    ChainLength = count()
  by CorrelationId
| where ChainLength > 1
| order by ChainLength desc
| take 20

// Error spike detection
ErrorEvents
| where Timestamp > ago(7d)
| summarize ErrorCount = count() by bin(Timestamp, 1h)
| extend Threshold = avg_if(ErrorCount, Timestamp < ago(1d)) * 3
| where Timestamp > ago(1d) and ErrorCount > Threshold

// Top recurring errors
ErrorEvents
| where Timestamp > ago(7d)
| summarize Count = count(), FirstSeen = min(Timestamp),
    LastSeen = max(Timestamp), AffectedItems = dcount(ItemName)
  by ErrorCode, ErrorCategory
| order by Count desc
| take 25
```

### Step 4: Materialized Views

```kql
.create materialized-view ErrorSummaryHourly on table ErrorEvents {
    ErrorEvents
    | summarize ErrorCount = count(), UniqueErrors = dcount(ErrorCode),
        AffectedItems = dcount(ItemName)
      by WorkloadType, ErrorCategory, bin(Timestamp, 1h)
}
```

## Error Triage Workflow

```text
1. Alert fires (Data Activator or manual check)
2. Open Error Tracking dashboard
3. Identify top error category from ErrorSummaryHourly
4. Drill into CorrelationId to trace error chain
5. Check ErrorBudget view for SLA impact
6. Resolve root cause, verify via ErrorRate5Min view
```

## Configuration

| Setting | Recommended | Description |
|---------|-------------|-------------|
| Error retention | 180 days | Long enough for trend analysis |
| Hot cache | 30 days | Fast queries on recent errors |
| Severity levels | Critical, Warning, Info | Three-tier classification |
| Correlation ID | Required on all events | Enables cross-stage tracing |

## Common Mistakes

### Wrong

```text
Logging only error messages without correlation IDs
--> Cannot trace errors across pipeline stages
```

### Correct

```text
Include CorrelationId, WorkloadType, and ErrorCode in every error event
--> Enables cross-stage root cause analysis and deduplication
```

## Related

- [Alerting Rules](../concepts/alerting-rules.md)
- [Eventhouse Basics](../concepts/eventhouse-basics.md)
- [Workspace Monitoring](workspace-monitoring.md)
- [Real-Time Dashboard](real-time-dashboard.md)
