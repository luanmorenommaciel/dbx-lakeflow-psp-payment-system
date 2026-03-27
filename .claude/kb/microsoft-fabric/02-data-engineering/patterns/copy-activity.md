> **MCP Validated:** 2026-02-17

# Copy Activity

> **Purpose**: Data Factory Copy Activity patterns for ingesting data into Fabric Lakehouse and Warehouse

## When to Use

- Moving data from external sources (SQL Server, Azure SQL, Blob, S3) into Fabric
- Bulk loading CSV, Parquet, or JSON files into Lakehouse Tables or Files
- Implementing incremental data loading with watermark patterns
- Low-code/no-code ETL for petabyte-scale data movement

## Implementation

```json
{
  "name": "Copy_SQLServer_to_Lakehouse",
  "type": "Copy",
  "inputs": [
    {
      "referenceName": "SqlServerSource",
      "type": "DatasetReference"
    }
  ],
  "outputs": [
    {
      "referenceName": "LakehouseDelta",
      "type": "DatasetReference"
    }
  ],
  "typeProperties": {
    "source": {
      "type": "SqlServerSource",
      "sqlReaderQuery": {
        "value": "SELECT * FROM dbo.orders WHERE modified_date > '@{pipeline().parameters.LastWatermark}'",
        "type": "Expression"
      },
      "partitionOption": "DynamicRange",
      "partitionSettings": {
        "partitionColumnName": "order_id",
        "partitionUpperBound": 10000000,
        "partitionLowerBound": 1
      }
    },
    "sink": {
      "type": "LakehouseTableSink",
      "tableActionOption": "Append",
      "partitionOption": "None"
    },
    "enableStaging": false,
    "parallelCopies": 8
  }
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `parallelCopies` | Auto | Degree of copy parallelism (1-256) |
| `tableActionOption` | Append | Options: Append, Overwrite |
| `enableStaging` | false | Use staging for cross-region copies |
| `partitionOption` | None | Options: None, DynamicRange, PhysicalPartitions |
| `writeBatchSize` | 10000 | Rows per batch write |
| `writeBatchTimeout` | 00:30:00 | Timeout per batch |

## Example Usage

```python
# Python: Create and run a Copy Activity pipeline via REST API
import requests
import time

def create_copy_pipeline(workspace_id: str, headers: dict):
    """Create a pipeline with Copy Activity for incremental loading."""
    pipeline_def = {
        "displayName": "IncrementalLoad_Orders",
        "definition": {
            "parts": [
                {
                    "path": "pipeline-content.json",
                    "payload": '{"activities":[{"name":"CopyOrders","type":"Copy","typeProperties":{"source":{"type":"SqlServerSource","sqlReaderQuery":"SELECT * FROM dbo.orders WHERE modified_date > @pipeline().parameters.watermark"},"sink":{"type":"LakehouseTableSink","tableActionOption":"Append"}}}],"parameters":{"watermark":{"type":"String","defaultValue":"1900-01-01"}}}'
                }
            ]
        }
    }
    response = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/dataPipelines",
        headers=headers,
        json=pipeline_def
    )
    return response.json()

def run_pipeline(workspace_id: str, pipeline_id: str, headers: dict, watermark: str):
    """Execute a pipeline run with parameters."""
    payload = {
        "executionData": {
            "parameters": {"watermark": watermark}
        }
    }
    response = requests.post(
        f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items/{pipeline_id}/jobs/instances?jobType=Pipeline",
        headers=headers,
        json=payload
    )
    return response.json()
```

## Delivery Styles

| Style | Use Case | Pattern |
|-------|----------|---------|
| Full copy | Initial load, small tables | Overwrite mode |
| Incremental | Large tables, daily loads | Watermark column + Append |
| CDC replication | Near-real-time sync | Change data capture mode |
| Partitioned copy | Billion-row tables | DynamicRange partition |

## Performance Tips

| Tip | Impact |
|-----|--------|
| Enable parallel copies (8-32) | 3-10x throughput increase |
| Use DynamicRange partitioning | Splits large reads across threads |
| Choose Parquet sink format | 2-5x faster than CSV |
| Enable staging for cross-region | Avoids timeout on large transfers |
| Avoid SELECT * | Reduce network transfer |

## See Also

- [Lakehouse](../concepts/lakehouse.md)
- [Medallion in Fabric](../../03-architecture-patterns/patterns/medallion-fabric.md)
- [SDK Automation](../../05-apis-sdks/patterns/sdk-automation.md)
