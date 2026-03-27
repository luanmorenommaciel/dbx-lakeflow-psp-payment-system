# BigQuery Terraform Module

> **Purpose**: Reusable module for creating BigQuery datasets, tables, and access controls
> **MCP Validated**: 2026-02-17

## When to Use

- Provisioning BigQuery datasets and tables for data pipelines
- Standardizing schema management across environments
- Implementing access controls and table expiration policies

## Implementation

```hcl
# modules/bigquery/variables.tf
variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "dataset_id" {
  type        = string
  description = "BigQuery dataset ID"
}

variable "location" {
  type    = string
  default = "US"
}

variable "description" {
  type    = string
  default = ""
}

variable "default_table_expiration_ms" {
  type    = number
  default = null
}

variable "delete_contents_on_destroy" {
  type    = bool
  default = false
}

variable "tables" {
  type = map(object({
    schema_file            = optional(string, null)
    schema_json            = optional(string, null)
    time_partitioning_field = optional(string, null)
    time_partitioning_type  = optional(string, "DAY")
    clustering_fields       = optional(list(string), [])
    expiration_time         = optional(number, null)
  }))
  default = {}
}

variable "access" {
  type = list(object({
    role          = string
    user_by_email = optional(string)
    group_by_email = optional(string)
    special_group  = optional(string)
  }))
  default = []
}

variable "labels" {
  type    = map(string)
  default = {}
}
```

```hcl
# modules/bigquery/main.tf
resource "google_bigquery_dataset" "dataset" {
  project                    = var.project_id
  dataset_id                 = var.dataset_id
  location                   = var.location
  description                = var.description
  default_table_expiration_ms = var.default_table_expiration_ms
  delete_contents_on_destroy  = var.delete_contents_on_destroy
  labels                      = var.labels

  dynamic "access" {
    for_each = var.access
    content {
      role           = access.value.role
      user_by_email  = access.value.user_by_email
      group_by_email = access.value.group_by_email
      special_group  = access.value.special_group
    }
  }
}

resource "google_bigquery_table" "tables" {
  for_each = var.tables

  project    = var.project_id
  dataset_id = google_bigquery_dataset.dataset.dataset_id
  table_id   = each.key
  schema     = each.value.schema_file != null ? file(each.value.schema_file) : each.value.schema_json

  deletion_protection = false

  dynamic "time_partitioning" {
    for_each = each.value.time_partitioning_field != null ? [1] : []
    content {
      type  = each.value.time_partitioning_type
      field = each.value.time_partitioning_field
    }
  }

  clustering = length(each.value.clustering_fields) > 0 ? each.value.clustering_fields : null
}
```

```hcl
# modules/bigquery/outputs.tf
output "dataset_id" {
  value = google_bigquery_dataset.dataset.dataset_id
}

output "dataset_self_link" {
  value = google_bigquery_dataset.dataset.self_link
}

output "table_ids" {
  value = { for k, v in google_bigquery_table.tables : k => v.table_id }
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `location` | `US` | Dataset location (must match bucket for loads) |
| `default_table_expiration_ms` | `null` | Auto-delete tables after N ms |
| `delete_contents_on_destroy` | `false` | Allow destroy with data |
| `time_partitioning_type` | `DAY` | DAY, HOUR, MONTH, or YEAR |

## Example Usage

```hcl
module "analytics_dataset" {
  source = "./modules/bigquery"

  project_id = var.project_id
  dataset_id = "analytics_${var.environment}"
  location   = "US"

  tables = {
    invoices = {
      schema_file             = "${path.module}/schemas/invoices.json"
      time_partitioning_field  = "created_at"
      time_partitioning_type   = "DAY"
      clustering_fields        = ["customer_id", "status"]
    }
    events = {
      schema_file             = "${path.module}/schemas/events.json"
      time_partitioning_field  = "event_timestamp"
      clustering_fields        = ["event_type"]
    }
  }

  access = [
    {
      role          = "OWNER"
      special_group = "projectOwners"
    },
    {
      role           = "READER"
      group_by_email = "data-analysts@company.com"
    }
  ]

  labels = {
    domain      = "analytics"
    environment = var.environment
  }
}
```

## See Also

- [GCS Module](../patterns/gcs-module.md)
- [IAM Module](../patterns/iam-module.md)
- [Resources](../concepts/resources.md)
