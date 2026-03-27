> **MCP Validated:** 2026-02-17

# Dataflow Gen2

> **Purpose**: Dataflow Gen2 (Power Query Online) for low-code data transformations, M language, staging, and incremental refresh
> **Confidence**: 0.95

## Overview

Dataflow Gen2 is Fabric's low-code ETL tool built on Power Query Online. It provides a visual transformation interface backed by the M language (functional, case-sensitive). Dataflows ingest from 150+ connectors, apply transformations, and output to Lakehouses, Warehouses, or KQL databases. Gen2 introduces staging (via Lakehouse intermediate storage) for improved performance and chaining with data pipelines.

## M Language Basics

```m
let
    // Connect to source
    Source = Csv.Document(
        Web.Contents("https://data.example.com/invoices.csv"),
        [Delimiter=",", Columns=6, Encoding=65001]
    ),
    // Promote headers and set types
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    Typed = Table.TransformColumnTypes(Headers, {
        {"invoice_id", type text}, {"amount", type number},
        {"invoice_date", type date}, {"vendor_name", type text},
        {"status", type text}, {"line_items", Int64.Type}
    }),
    // Filter and compute
    Filtered = Table.SelectRows(Typed, each [status] = "approved" and [amount] > 0),
    WithTax = Table.AddColumn(Filtered, "amount_with_tax",
        each [amount] * 1.15, type number)
in WithTax
```

## Staging vs Direct Query

| Mode | How It Works | When to Use |
|------|-------------|-------------|
| **Staging** (default) | Data lands in staging Lakehouse then loads to destination | Large datasets, complex transforms |
| **Direct query** | Data flows directly from source to destination | Small datasets, simple transforms |

## Incremental Refresh

```m
// RangeStart and RangeEnd are injected by Fabric
let
    Source = Sql.Database("server.database.windows.net", "sales_db"),
    Orders = Source{[Schema="dbo", Item="orders"]}[Data],
    Filtered = Table.SelectRows(Orders, each
        [modified_date] >= RangeStart and [modified_date] < RangeEnd
    )
in Filtered
// Configure in UI: refresh range (7 days), full range (3 years), detect changes column
```

## Common Transformations

| Operation | M Function | Example |
|-----------|-----------|---------|
| Filter rows | `Table.SelectRows` | `each [amount] > 100` |
| Add column | `Table.AddColumn` | `each [qty] * [price]` |
| Remove column | `Table.RemoveColumns` | `{"temp_col"}` |
| Group by | `Table.Group` | `{"Region", {"Total", each List.Sum([Sales])}}` |
| Merge (join) | `Table.NestedJoin` | Left, inner, outer joins |
| Pivot | `Table.Pivot` | Rows to columns |
| Unpivot | `Table.UnpivotOtherColumns` | Columns to rows |
| Replace values | `Table.ReplaceValue` | null to default |

## Configuration

| Setting | Options | Description |
|---------|---------|-------------|
| Destination | Lakehouse, Warehouse, KQL DB | Where transformed data lands |
| Update method | Replace, Append | How data is written |
| Staging | Enabled (default), Disabled | Intermediate storage |
| Timeout | 30 min default | Maximum execution time |

## Common Mistakes

### Wrong

```m
// Table.Buffer breaks query folding -- full table loaded into memory
let Buffered = Table.Buffer(AllRows),
    Filtered = Table.SelectRows(Buffered, each [date] > #date(2025, 1, 1))
in Filtered
```

### Correct

```m
// Filter before non-foldable operations (pushes filter to source)
let Filtered = Table.SelectRows(AllRows, each [date] > #date(2025, 1, 1))
in Filtered
```

## Related

- [Lakehouse](lakehouse.md)
- [Spark Notebooks](spark-notebooks.md)
- [Copy Activity](../patterns/copy-activity.md)
- [Shortcuts](shortcuts.md)
