# GCS Bucket Terraform Module

> **Purpose**: Reusable module for creating GCS buckets with lifecycle rules, versioning, and IAM
> **MCP Validated**: 2026-02-17

## When to Use

- Creating storage buckets for data pipelines
- Standardizing bucket configuration across environments
- Implementing lifecycle policies for cost management

## Implementation

```hcl
# modules/gcs/variables.tf
variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique bucket name"
}

variable "location" {
  type        = string
  default     = "US"
  description = "Bucket location (region, dual-region, or multi-region)"
}

variable "storage_class" {
  type    = string
  default = "STANDARD"
  validation {
    condition     = contains(["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"], var.storage_class)
    error_message = "Must be STANDARD, NEARLINE, COLDLINE, or ARCHIVE."
  }
}

variable "enable_versioning" {
  type    = bool
  default = true
}

variable "force_destroy" {
  type    = bool
  default = false
}

variable "lifecycle_rules" {
  type = list(object({
    action_type          = string
    action_storage_class = optional(string)
    condition_age        = optional(number)
    condition_with_state = optional(string, "ANY")
  }))
  default = []
}

variable "labels" {
  type    = map(string)
  default = {}
}
```

```hcl
# modules/gcs/main.tf
resource "google_storage_bucket" "bucket" {
  name     = var.bucket_name
  project  = var.project_id
  location = var.location

  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  force_destroy               = var.force_destroy
  labels                      = var.labels

  versioning {
    enabled = var.enable_versioning
  }

  dynamic "lifecycle_rule" {
    for_each = var.lifecycle_rules
    content {
      action {
        type          = lifecycle_rule.value.action_type
        storage_class = lifecycle_rule.value.action_storage_class
      }
      condition {
        age        = lifecycle_rule.value.condition_age
        with_state = lifecycle_rule.value.condition_with_state
      }
    }
  }
}
```

```hcl
# modules/gcs/outputs.tf
output "bucket_name" {
  value = google_storage_bucket.bucket.name
}

output "bucket_url" {
  value = google_storage_bucket.bucket.url
}

output "bucket_self_link" {
  value = google_storage_bucket.bucket.self_link
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `location` | `US` | Multi-region, dual-region, or region |
| `storage_class` | `STANDARD` | Storage tier for pricing/access |
| `enable_versioning` | `true` | Object version history |
| `force_destroy` | `false` | Allow deletion with objects |
| `uniform_bucket_level_access` | `true` | Uniform IAM (always enabled) |

## Example Usage

```hcl
module "data_lake" {
  source = "./modules/gcs"

  project_id  = var.project_id
  bucket_name = "${var.project_id}-${var.environment}-data-lake"
  location    = "US"

  lifecycle_rules = [
    {
      action_type          = "SetStorageClass"
      action_storage_class = "NEARLINE"
      condition_age        = 30
    },
    {
      action_type          = "SetStorageClass"
      action_storage_class = "COLDLINE"
      condition_age        = 90
    },
    {
      action_type   = "Delete"
      condition_age = 365
    }
  ]

  labels = {
    purpose     = "data-lake"
    environment = var.environment
  }
}

module "raw_uploads" {
  source = "./modules/gcs"

  project_id    = var.project_id
  bucket_name   = "${var.project_id}-${var.environment}-raw-uploads"
  location      = var.region
  force_destroy = var.environment != "prod"

  lifecycle_rules = [
    {
      action_type   = "Delete"
      condition_age = 7
    }
  ]
}
```

## See Also

- [BigQuery Module](../patterns/bigquery-module.md)
- [Remote State](../patterns/remote-state.md)
- [Resources](../concepts/resources.md)
