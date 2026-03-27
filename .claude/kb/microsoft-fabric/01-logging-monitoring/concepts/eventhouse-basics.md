> **MCP Validated:** 2026-02-17

# Eventhouse Basics

> **Purpose**: Eventhouse architecture, KQL databases, data ingestion, and real-time analytics in Microsoft Fabric
> **Confidence**: 0.95

## Overview

An Eventhouse is a real-time analytics engine in Microsoft Fabric optimized for streaming and time-series data. It hosts one or more KQL databases, each containing tables, functions, and materialized views. Eventhouses use a columnar storage engine with automatic indexing, making them ideal for high-throughput ingestion (millions of events per second) and sub-second analytical queries over terabytes of data.

## Architecture

```text
Eventhouse
├── KQL Database 1 (e.g., telemetry_db)
│   ├── Tables (raw event storage)
│   ├── Functions (reusable query logic)
│   ├── Materialized Views (pre-aggregated)
│   └── External Tables (shortcuts)
├── KQL Database 2 (e.g., monitoring_db)
│   └── ...
└── Ingestion Endpoints
    ├── Eventstream (real-time)
    ├── One-time ingestion (batch)
    ├── Queued ingestion (API)
    └── Shortcuts (OneLake)
```

## Creating KQL Databases and Tables

```kql
// Create a table with typed columns
.create table PipelineEvents (
    Timestamp: datetime,
    PipelineName: string,
    Status: string,
    DurationMs: long,
    RowsProcessed: long,
    ErrorMessage: string,
    WorkspaceId: guid
)

// Set retention policy -- keep data for 90 days
.alter-merge table PipelineEvents policy retention
```
```json
{ "SoftDeletePeriod": "90.00:00:00", "Recoverability": "Enabled" }
```

```kql
// Set caching policy -- keep 30 days in hot cache
.alter table PipelineEvents policy caching hot = 30d

// Create ingestion mapping for JSON sources
.create table PipelineEvents ingestion json mapping 'PipelineEventsMapping'
'['
'  {"column":"Timestamp","path":"$.timestamp","datatype":"datetime"},'
'  {"column":"PipelineName","path":"$.pipeline_name","datatype":"string"},'
'  {"column":"Status","path":"$.status","datatype":"string"},'
'  {"column":"DurationMs","path":"$.duration_ms","datatype":"long"},'
'  {"column":"RowsProcessed","path":"$.rows_processed","datatype":"long"},'
'  {"column":"ErrorMessage","path":"$.error_message","datatype":"string"},'
'  {"column":"WorkspaceId","path":"$.workspace_id","datatype":"guid"}'
']'

// Create a materialized view for hourly aggregation
.create materialized-view PipelineHourlySummary on table PipelineEvents {
    PipelineEvents
    | summarize
        TotalRuns = count(),
        FailedRuns = countif(Status == "Failed"),
        AvgDurationMs = avg(DurationMs),
        TotalRows = sum(RowsProcessed)
      by PipelineName, bin(Timestamp, 1h)
}
```

## Quick Reference

| Feature | Description |
|---------|-------------|
| **Hot cache** | In-memory/SSD storage for recent data (sub-second queries) |
| **Cold storage** | OneLake-backed long-term storage |
| **Retention** | Auto-delete data older than SoftDeletePeriod |
| **Streaming ingestion** | Sub-second latency via Eventstream |
| **Batched ingestion** | Higher throughput, 3-5 min latency |
| **OneLake availability** | Tables mirrored to OneLake in Delta format |

## Ingestion Methods

| Method | Latency | Use Case |
|--------|---------|----------|
| Eventstream | Seconds | IoT, clickstream, real-time telemetry |
| Get Data (UI) | Minutes | One-time loads, CSV/JSON uploads |
| Queued ingestion API | 3-5 min | Programmatic batch ingestion |
| OneLake shortcut | N/A | Query existing Delta tables in-place |
| Data pipeline | Minutes | Scheduled ETL from external sources |

## Retention Policies

| Policy | Purpose | Default |
|--------|---------|---------|
| **SoftDeletePeriod** | How long data is kept before deletion | 36,500 days |
| **HotCachePeriod** | How long data stays in hot cache | 31 days |
| **Recoverability** | Whether soft-deleted data can be recovered | Enabled |

## Common Mistakes

### Wrong

```kql
// Querying without time filter on large tables
PipelineEvents
| summarize count() by PipelineName
```

### Correct

```kql
// Always include a time filter for performance
PipelineEvents
| where Timestamp > ago(7d)
| summarize count() by PipelineName
```

## Related

- [KQL Queries](kql-queries.md)
- [Alerting Rules](alerting-rules.md)
- [Workspace Monitoring](../patterns/workspace-monitoring.md)
- [Real-Time Dashboard](../patterns/real-time-dashboard.md)
