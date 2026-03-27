> **MCP Validated:** 2026-02-17

# Real-Time Dashboard

> **Purpose**: Pattern for building real-time monitoring dashboards using Eventhouse + KQL + Power BI DirectQuery

## When to Use

- Monitoring pipeline health, latency, and error rates in near-real-time
- Building operational dashboards that auto-refresh every few seconds
- Visualizing streaming telemetry from Eventstreams into Eventhouse
- Creating NOC-style status boards for data platform operations

## Architecture

```text
Eventstream ──▶ Eventhouse (KQL DB) ──▶ Power BI (DirectQuery) ──▶ Dashboard

DirectQuery Mode: Every visual sends a live KQL query on each
refresh cycle (configurable 1s-30min)
```

## Implementation

### Step 1: Materialized Views for Dashboard Queries

```kql
// Hourly pipeline health summary
.create materialized-view PipelineHealthHourly on table PipelineEvents {
    PipelineEvents
    | summarize
        TotalRuns = count(),
        SuccessCount = countif(Status == "Completed"),
        FailureCount = countif(Status == "Failed"),
        AvgDurationMs = avg(DurationMs),
        P95DurationMs = percentile(DurationMs, 95)
      by PipelineName, bin(Timestamp, 1h)
}

// Error rate per 5-minute window
.create materialized-view ErrorRate5Min on table PipelineEvents {
    PipelineEvents
    | summarize Total = count(), Errors = countif(Status == "Failed")
      by bin(Timestamp, 5m)
    | extend ErrorRate = round(100.0 * Errors / Total, 2)
}
```

### Step 2: KQL Queries for Dashboard Visuals

```kql
// Visual 1: Current pipeline status (last 1 hour)
PipelineEvents
| where Timestamp > ago(1h)
| summarize LastStatus = arg_max(Timestamp, Status), RunCount = count()
  by PipelineName
| project PipelineName, Status, RunCount
| extend StatusIcon = case(Status == "Completed", "OK",
    Status == "Failed", "FAIL", "RUN")

// Visual 2: Latency trend (last 24h, 15-min buckets)
PipelineEvents
| where Timestamp > ago(24h) and Status == "Completed"
| summarize P50 = percentile(DurationMs, 50),
    P95 = percentile(DurationMs, 95),
    P99 = percentile(DurationMs, 99)
  by bin(Timestamp, 15m)
| render timechart

// Visual 3: Error rate over time (last 12 hours)
PipelineEvents
| where Timestamp > ago(12h)
| summarize Total = count(), Errors = countif(Status == "Failed")
  by bin(Timestamp, 10m)
| extend ErrorRatePct = round(100.0 * Errors / Total, 2)
| render areachart

// Visual 4: Top errors by pipeline (last 24 hours)
PipelineEvents
| where Timestamp > ago(24h) and Status == "Failed"
| summarize ErrorCount = count() by PipelineName, ErrorMessage
| top 20 by ErrorCount desc

// Visual 5: Throughput rows processed per hour
PipelineEvents
| where Timestamp > ago(24h) and Status == "Completed"
| summarize TotalRows = sum(RowsProcessed) by bin(Timestamp, 1h)
| render columnchart
```

### Step 3: Reusable KQL Function

```kql
.create-or-alter function PipelineHealthScore(lookback: timespan) {
    PipelineEvents
    | where Timestamp > ago(lookback)
    | summarize Total = count(), Success = countif(Status == "Completed")
      by PipelineName
    | extend HealthPct = round(100.0 * Success / Total, 1)
    | extend HealthStatus = case(
        HealthPct >= 99, "Healthy", HealthPct >= 95, "Degraded", "Critical")
}
```

### Step 4: Power BI DirectQuery Connection

```text
1. Power BI > Get Data > Microsoft Fabric > KQL Database
2. Select the Eventhouse and KQL database
3. Choose DirectQuery mode (NOT Import)
4. Write KQL queries in the query editor
5. Set auto page refresh: 10-30 seconds recommended
```

## Dashboard Layout

```text
┌──────────────────────────────────────────────────┐
│  PIPELINE HEALTH DASHBOARD       Auto-refresh: 30s│
├──────────┬──────────┬──────────┬─────────────────┤
│ Success  │ Error    │  P95     │ Rows Processed  │
│  Rate    │  Count   │ Latency  │  (last hour)    │
│  98.5%   │   3      │  4.2s    │  1,245,000      │
├──────────┴──────────┴──────────┴─────────────────┤
│  Latency Trend (24h)                 [timechart]  │
├──────────────────────┬───────────────────────────┤
│  Error Rate (12h)    │  Pipeline Status Grid      │
│  [areachart]         │  Pipeline A: OK            │
│                      │  Pipeline B: FAIL          │
├──────────────────────┴───────────────────────────┤
│  Top Errors (table)                               │
└──────────────────────────────────────────────────┘
```

## Configuration

| Setting | Recommended | Description |
|---------|-------------|-------------|
| Auto-refresh interval | 10-30s | Balance freshness vs query load |
| Materialized views | Yes | Pre-aggregate for faster rendering |
| DirectQuery mode | Required | Enables real-time data access |
| Row limit per visual | 10,000 | Prevent slow-rendering visuals |

## Common Mistakes

### Wrong

```text
Using Import mode for real-time dashboards
--> Data only refreshes on scheduled intervals (min 15 min)
```

### Correct

```text
Use DirectQuery mode against Eventhouse KQL database
--> Data refreshes on every auto-refresh cycle (as low as 1s)
```

## Related

- [Eventhouse Basics](../concepts/eventhouse-basics.md)
- [KQL Queries](../concepts/kql-queries.md)
- [Workspace Monitoring](workspace-monitoring.md)
- [Error Tracking](error-tracking.md)
