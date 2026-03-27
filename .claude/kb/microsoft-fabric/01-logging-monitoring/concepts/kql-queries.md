> **MCP Validated:** 2026-02-17

# KQL Queries

> **Purpose**: Kusto Query Language fundamentals for Fabric monitoring and real-time intelligence
> **Confidence**: 0.95

## Overview

KQL (Kusto Query Language) is the primary query language for real-time intelligence in Microsoft Fabric. It powers Eventhouse queries, workspace monitoring diagnostics, and time-series analytics. KQL uses a pipe-based syntax where data flows through operators sequentially, making it intuitive for log analysis and streaming workloads.

## The Pattern

```kql
// Basic KQL query structure: table | operator | operator
StormEvents
| where StartTime between (datetime(2024-01-01) .. datetime(2024-12-31))
| where DamageProperty > 0
| summarize TotalDamage = sum(DamageProperty), EventCount = count() by State
| top 10 by TotalDamage desc
| render barchart

// Time-series aggregation
Traces
| where timestamp > ago(24h)
| summarize RequestCount = count() by bin(timestamp, 1h)
| render timechart

// Pattern detection with dynamic parsing
CustomLogs
| where Level == "Error"
| parse Message with "Operation " OperationName " failed with code " ErrorCode
| summarize ErrorCount = count() by OperationName, ErrorCode
| order by ErrorCount desc
```

## Quick Reference

| Operator | Purpose | Example |
|----------|---------|---------|
| `where` | Filter rows | `where Status == "Failed"` |
| `summarize` | Aggregate | `summarize count() by Region` |
| `extend` | Add column | `extend Duration = EndTime - StartTime` |
| `project` | Select columns | `project Name, Status, Duration` |
| `top` | Limit + sort | `top 10 by Count desc` |
| `bin()` | Time bucketing | `bin(timestamp, 1h)` |
| `ago()` | Relative time | `where timestamp > ago(7d)` |
| `parse` | Extract fields | `parse Msg with * "id=" Id` |
| `render` | Visualize | `render timechart` |
| `join` | Combine tables | `join kind=inner T2 on Key` |

## Common Mistakes

### Wrong

```kql
// Using SQL-style syntax in KQL
SELECT * FROM StormEvents WHERE State = 'TEXAS'
```

### Correct

```kql
// KQL pipe-based syntax
StormEvents
| where State == "TEXAS"
```

## Fabric-Specific KQL Tables

| Table | Source | Use Case |
|-------|--------|----------|
| `ItemJobEventLogs` | Workspace monitoring | Pipeline job tracking |
| `SemanticModelLogs` | Power BI models | Query performance |
| `EventhouseQueryLogs` | Eventhouse | KQL query audit |
| `EventhouseCommandLogs` | Eventhouse | Command audit |

## Related

- [Workspace Monitoring](../patterns/workspace-monitoring.md)
- [Lakehouse](../../02-data-engineering/concepts/lakehouse.md)
