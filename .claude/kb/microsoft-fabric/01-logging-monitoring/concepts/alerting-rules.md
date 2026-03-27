> **MCP Validated:** 2026-02-17

# Alerting Rules

> **Purpose**: Data Activator reflex rules, alert conditions, trigger types, and notification channels in Microsoft Fabric
> **Confidence**: 0.95

## Overview

Data Activator is the alerting and action engine in Microsoft Fabric. It uses **Reflex** items to monitor data streams and trigger automated actions when conditions are met. Reflexes connect to Eventstreams, Power BI visuals, or Fabric events, evaluate conditions continuously, and send notifications via email, Teams, or Power Automate flows.

## Trigger Types

| Type | Description | Example |
|------|-------------|---------|
| **Threshold** | Value crosses a numeric boundary | Error count > 10 |
| **Change** | Value changes by amount or percentage | Latency increases by 50% |
| **Absence** | No data received within time window | No events for 15 minutes |
| **Pattern** | Repeated condition over time | Failures 3 times in 1 hour |

## Defining a Reflex Rule

```text
1. Create a Reflex item in your Fabric workspace
2. Connect to a data source (Eventstream or Power BI report)
3. Define an Object (entity being monitored) with an ID column
4. Add Properties (numeric or categorical fields to track)
5. Create a Trigger with condition logic
6. Configure the Action (notification channel)
```

## Example Alert Definitions

### Pipeline Failure Alert

```yaml
reflex: pipeline-health-monitor
object: Pipeline
  id_column: PipelineName
  properties:
    - name: FailureCount
      source: Eventstream
      aggregation: countif(Status == "Failed")
      window: 1h
trigger: high-failure-rate
  condition: FailureCount > 5
  evaluation_interval: 5m
  action:
    type: Teams
    channel: "#data-ops-alerts"
    message: "Pipeline {{PipelineName}} has {{FailureCount}} failures in the last hour"
```

### Latency Spike Alert

```yaml
reflex: latency-monitor
object: PipelineStage
  id_column: StageName
  properties:
    - name: P95Latency
      source: Eventstream
      aggregation: percentile(DurationMs, 95)
      window: 30m
trigger: latency-spike
  condition: P95Latency > 30000
  evaluation_interval: 10m
  action:
    type: Email
    recipients: ["data-team@company.com"]
    subject: "Latency spike: {{StageName}} P95 = {{P95Latency}}ms"
```

### Data Freshness Alert

```yaml
reflex: freshness-monitor
object: LakehouseTable
  id_column: TableName
  properties:
    - name: LastRefreshTime
      aggregation: max(Timestamp)
trigger: stale-data
  condition: now() - LastRefreshTime > 2h
  evaluation_interval: 15m
  action:
    type: PowerAutomate
    flow_id: "abc-123-def"
```

## Notification Channels

| Channel | Best For |
|---------|----------|
| **Email** | Individual alerts, on-call rotation |
| **Teams** | Team-wide visibility, collaboration |
| **Power Automate** | Complex workflows, ticket creation, escalation |
| **Custom endpoint** | Integration with PagerDuty, Slack, Opsgenie |

## Configuration

| Setting | Options | Description |
|---------|---------|-------------|
| Evaluation interval | 1m to 1h | How often the condition is checked |
| Lookback window | 5m to 24h | Time range for aggregation |
| Snooze period | 0m to 24h | Suppress repeat alerts after firing |
| Severity | Info, Warning, Critical | Alert classification |

## Common Mistakes

### Wrong

```text
Creating a trigger with no snooze period on a noisy metric
--> Results in hundreds of duplicate alerts per hour
```

### Correct

```text
Set snooze period to at least the evaluation interval
--> Critical alerts: snooze 15m, Warning alerts: snooze 1h
```

## Related

- [KQL Queries](kql-queries.md)
- [Eventhouse Basics](eventhouse-basics.md)
- [Workspace Monitoring](../patterns/workspace-monitoring.md)
- [Error Tracking](../patterns/error-tracking.md)
