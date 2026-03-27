# Resources

> **Purpose**: Terraform resource blocks for declaring and managing GCP infrastructure
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Resources are the most important element in Terraform. Each resource block declares
a single infrastructure object that Terraform manages throughout its lifecycle.
Resources map directly to GCP API objects and support create, read, update, and delete operations.

## The Pattern

```hcl
resource "google_cloud_run_v2_service" "api" {
  name     = "${var.project_prefix}-api"
  location = var.region
  ingress  = "INGRESS_TRAFFIC_ALL"

  template {
    containers {
      image = var.container_image

      resources {
        limits = {
          cpu    = "1000m"
          memory = "512Mi"
        }
      }

      env {
        name  = "PROJECT_ID"
        value = var.project_id
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 10
    }
  }

  lifecycle {
    ignore_changes = [
      template[0].containers[0].image,
    ]
  }
}
```

## Meta-Arguments

| Argument | Purpose | Example |
|----------|---------|---------|
| `depends_on` | Explicit dependency | `depends_on = [google_project_service.run]` |
| `count` | Conditional creation | `count = var.enable_feature ? 1 : 0` |
| `for_each` | Multiple instances from map | `for_each = var.services` |
| `lifecycle` | Control create/destroy behavior | `prevent_destroy = true` |
| `provider` | Non-default provider | `provider = google.us_east` |

## Ephemeral Resources (Terraform 1.10+)

Ephemeral resources are temporary -- Terraform does not store them in state or plan files.
Use them for secrets, short-lived tokens, and temporary credentials.

```hcl
# Ephemeral resource: password never stored in state
ephemeral "random_password" "db_password" {
  length           = 16
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# Write-only argument (1.11+): password_wo is never persisted
resource "google_sql_database_instance" "main" {
  name             = "main-instance"
  database_version = "POSTGRES_15"
  region           = var.region

  settings {
    tier = "db-f1-micro"
  }
}

resource "google_sql_user" "admin" {
  name     = "admin"
  instance = google_sql_database_instance.main.name
  # password_wo keeps the value out of state (requires provider support)
  password = ephemeral.random_password.db_password.result
}
```

### Ephemeral with for_each

```hcl
locals {
  environments = toset(["dev", "staging", "prod"])
}

ephemeral "random_password" "db_passwords" {
  for_each = local.environments
  length   = 16
}
```

## Lifecycle Rules

```hcl
resource "google_storage_bucket" "data" {
  name     = "${var.project_id}-data"
  location = var.region

  lifecycle {
    prevent_destroy = true   # Block accidental deletion
  }
}

resource "google_bigquery_table" "events" {
  dataset_id = google_bigquery_dataset.main.dataset_id
  table_id   = "events"

  lifecycle {
    create_before_destroy = true  # Zero-downtime replacement
    ignore_changes        = [schema]  # Schema managed externally
  }
}
```

## Common Mistakes

### Wrong

```hcl
# Hardcoded values, no naming convention
resource "google_storage_bucket" "bucket" {
  name     = "my-bucket"
  location = "US"
}
```

### Correct

```hcl
# Parameterized with consistent naming
resource "google_storage_bucket" "data_lake" {
  name     = "${var.project_id}-${var.environment}-data-lake"
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = var.environment != "prod"

  versioning {
    enabled = true
  }
}
```

## Data Sources

```hcl
# Read existing resources without managing them
data "google_project" "current" {}

data "google_service_account" "existing" {
  account_id = "my-service-account"
}
```

## Google Terraform Provider (v6.x)

The `hashicorp/google` provider v6.x (2025-2026) supports:

| Resource | Terraform Type | Notes |
|----------|---------------|-------|
| Cloud Run (GPU) | `google_cloud_run_v2_service` | `gpu` block in template |
| BigLake Iceberg table | `google_bigquery_table` | `table_constraints` for Iceberg |
| BigLake Metastore | `google_biglake_catalog` | Managed Iceberg catalog |
| Vertex AI Agent | `google_vertex_ai_agent` | Agent Builder resources |
| Dataflow Flex Template | `google_dataflow_flex_template_job` | GPU-enabled pipelines |

## Related

- [Modules](../concepts/modules.md)
- [Variables](../concepts/variables.md)
- [Cloud Run Module](../patterns/cloud-run-module.md)
