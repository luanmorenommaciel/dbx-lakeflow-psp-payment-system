# Serverless Lakeflow Pipelines

> Source: https://docs.databricks.com/aws/en/dlt/serverless-dlt
> Status: GA, recommended default. TCO reduced by up to 70% (July 2025 optimizations).

## Overview

**Serverless is the recommended compute option for Lakeflow Declarative Pipelines**, providing simplified infrastructure management with automatic scaling and optimization. As of July 2025, recent optimizations have cut TCO by up to 70%, making serverless the clear choice for most workloads.

## Key Benefits

### 🚀 Simplified Infrastructure Management
- Zero cluster configuration needed
- No manual tuning required
- Automatic resource provisioning

### 📈 Automatic Vertical Autoscaling
- Scales compute up/down automatically
- Optimizes for workload patterns
- No manual intervention needed

### 🔄 Incremental Refresh for Materialized Views
- Only processes changed data
- Reduces compute costs
- Faster update times

### ⚡ Concurrent Stream Pipelining
- Processes multiple streams in parallel
- Improved throughput
- Better resource utilization

### 🎯 Reduced Configuration Complexity
- Sensible defaults
- Less operational overhead
- Faster time to production

## Requirements

### Must-Have Prerequisites

✅ **Unity Catalog** must be enabled
✅ **Serverless terms of use** accepted
✅ **Workspace** in a serverless-enabled region

### Supported Regions

Check Databricks documentation for current list of serverless-enabled AWS regions.

## Configuration

### Default Setting

**Serverless is the default compute option** for new pipelines.

### Converting Existing Pipelines

Existing Unity Catalog pipelines can be converted to serverless:

1. Open pipeline configuration
2. Change compute setting to "Serverless"
3. Save and update pipeline

### No Manual Compute Settings

❌ Cannot configure:
- Worker count
- Instance types
- Cluster size
- Driver nodes

✅ Databricks manages all compute automatically

## Performance Modes

### Standard Mode (Default)

**Best for:** Cost-efficient workloads

**Characteristics:**
- Typically starts within 4-6 minutes
- Optimized for cost
- Good for scheduled/triggered pipelines

```json
{
  "serverless": true
}
```

### Performance Optimized Mode

**Best for:** Time-sensitive workloads

**Characteristics:**
- Faster startup times
- Higher compute allocation
- Premium pricing
- Real-time requirements

```json
{
  "serverless": true,
  "performance_mode": "OPTIMIZED"
}
```

## Features

### Automatic Vertical Autoscaling

```
Light workload → Smaller compute
Heavy workload → Larger compute
```

**Benefits:**
- No over-provisioning
- No under-provisioning
- Automatic optimization

### Concurrent Microbatch Processing

Processes multiple microbatches simultaneously:

```
Stream 1 ─┐
Stream 2 ─┼─→ Parallel Processing → Optimized Throughput
Stream 3 ─┘
```

### Incremental View Refreshes

```sql
CREATE OR REFRESH MATERIALIZED VIEW daily_sales AS
SELECT
    DATE(order_date) as sale_date,
    SUM(amount) as total_sales
FROM orders
GROUP BY DATE(order_date)
```

**On update:**
- Only processes new/changed data
- Skips unchanged partitions
- Dramatically faster refreshes

### Budget Policy Tagging (Preview)

Assign cost centers and track spending:

```json
{
  "serverless": true,
  "budget_policy": "sales_team_budget"
}
```

## Limitations

### ❌ Cannot Manually Configure Compute

No access to:
- Cluster settings
- Worker configuration
- Instance type selection
- Spark configurations

### ⚠️ AWS PrivateLink

Requires contacting Databricks representative for setup.

### 📍 Regional Availability

Some AWS regions may not support serverless pipelines yet.

### 💰 Pricing Model

Different pricing than classic compute:
- Pay for actual compute used
- Premium for performance mode
- No idle cluster costs

## When to Use Serverless

### ✅ Perfect For

1. **New pipelines** - Databricks recommendation
2. **Variable workloads** - Automatic scaling handles fluctuations
3. **Simplified operations** - Less operational overhead
4. **Fast iteration** - Quick setup and deployment
5. **Cost optimization** - Pay only for what you use

### 🤔 Consider Classic Compute For

1. **Very specific compute requirements** - Need precise cluster configuration
2. **Custom Spark configs** - Advanced tuning needed
3. **Unsupported regions** - Serverless not available
4. **Legacy compatibility** - Existing infrastructure constraints

## Migration Guide

### From Classic to Serverless

**Step 1: Verify Unity Catalog**
```sql
SHOW CATALOGS
```

**Step 2: Review Pipeline Configuration**
- Note any custom cluster settings
- Document Spark configurations
- Check library dependencies

**Step 3: Convert Pipeline**
1. Edit pipeline settings
2. Change compute to "Serverless"
3. Remove cluster-specific configurations
4. Save changes

**Step 4: Test**
1. Run pipeline in development mode
2. Verify performance
3. Check cost metrics
4. Validate output

**Step 5: Production Rollout**
1. Disable development mode
2. Set up monitoring
3. Configure alerts
4. Document changes

## Performance Optimization

### For Serverless Pipelines

**✅ DO:**

1. **Use appropriate refresh modes**
   - Continuous for real-time
   - Triggered for batch

2. **Leverage incremental processing**
   - Streaming tables for incremental data
   - Materialized views for aggregations

3. **Optimize data layout**
   - Partition large tables
   - Use Z-ordering

4. **Monitor performance**
   - Track update duration
   - Review cost metrics
   - Analyze bottlenecks

**❌ DON'T:**

1. **Don't use continuous mode** unnecessarily
2. **Don't process full refreshes** when incremental works
3. **Don't ignore** cost monitoring
4. **Don't skip** optimization opportunities

## Monitoring Serverless Pipelines

### Key Metrics

📊 **Update Duration** - How long updates take
💰 **DBU Consumption** - Cost tracking
📈 **Throughput** - Records processed per second
⚠️ **Failures** - Error rates and causes

### Access Metrics

**UI Dashboard:**
- Pipeline run history
- Cost breakdown
- Performance trends

**Event Logs:**
```python
events = spark.read.format("delta").load("/event_log_path")
metrics = events.filter("event_type = 'flow_progress'")
```

## Cost Optimization

### Strategies

1. **Use Triggered Mode** for batch workloads
2. **Optimize refresh schedules** - Not too frequent
3. **Leverage incremental processing** - Reduce data scanned
4. **Monitor DBU usage** - Identify expensive operations
5. **Use budget policies** - Set spending limits

### Cost Comparison

```
Classic Compute:
- Pay for cluster uptime (even when idle)
- Manual scaling decisions
- Fixed capacity costs

Serverless:
- Pay for actual compute used
- Automatic scaling
- No idle costs
- Premium for performance mode
```

## Best Practices

### ✅ DO

1. **Start with serverless** for new pipelines
2. **Use standard mode** unless you need performance mode
3. **Monitor costs** regularly
4. **Leverage automatic scaling** - trust the platform
5. **Use development mode** during development
6. **Set up budget alerts** for cost control

### ❌ DON'T

1. **Don't over-optimize** prematurely
2. **Don't use performance mode** by default
3. **Don't ignore** cost metrics
4. **Don't assume** classic patterns apply

## Troubleshooting

### Slow Startup Times

**Standard Mode:**
- Expected 4-6 minute startup
- Consider performance mode if critical

**Performance Mode:**
- Should be faster
- Contact support if consistently slow

### Higher Than Expected Costs

**Check:**
- Continuous vs triggered mode
- Refresh frequency
- Data volume processed
- Performance mode usage

**Optimize:**
- Switch to triggered mode
- Reduce refresh frequency
- Use incremental processing
- Use standard mode

### Regional Availability Issues

**If serverless not available:**
- Check supported regions list
- Consider workspace migration
- Use classic compute temporarily
- Contact Databricks support

## Example: Complete Serverless Pipeline

```json
{
  "name": "serverless_sales_pipeline",
  "serverless": true,
  "target": "production.sales",
  "configuration": {
    "source_path": "s3://raw-data/sales/",
    "catalog": "production"
  },
  "libraries": [
    {"notebook": {"path": "/Pipelines/sales_processing"}}
  ],
  "continuous": false,
  "development": false,
  "channel": "CURRENT",
  "edition": "ADVANCED",
  "notifications": [
    {
      "email_recipients": ["data-team@company.com"],
      "alerts": ["on-update-failure"]
    }
  ]
}
```

## Databricks Runtime Versions (2026)

| Channel | Runtime |
|---------|---------|
| CURRENT (default) | Databricks Runtime 16.4 |
| PREVIEW | Databricks Runtime 17.3 |

## Recommendation

**Databricks recommends developing new pipelines using serverless** for:
- Reduced operational complexity (up to 70% TCO reduction)
- Automatic optimization
- Cost efficiency
- Faster time to production
