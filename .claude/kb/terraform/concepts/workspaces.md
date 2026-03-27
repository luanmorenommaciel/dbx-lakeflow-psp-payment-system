# Workspaces

> **Purpose**: Environment isolation using shared Terraform configuration
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Terraform workspaces allow a single configuration to manage multiple distinct
sets of infrastructure. Each workspace has its own state file, enabling
environment separation (dev/staging/prod) without duplicating code. The GCS
backend natively supports workspaces with automatic state path separation.

## The Pattern

```hcl
# Backend with workspace-aware state paths
terraform {
  backend "gcs" {
    bucket = "my-project-terraform-state"
    prefix = "terraform/state"
    # State path: terraform/state/env:/<workspace>/default.tfstate
  }
}

# Use workspace name in resource configuration
locals {
  environment = terraform.workspace
  name_prefix = "${var.project_id}-${local.environment}"
  is_prod     = local.environment == "prod"
}

resource "google_cloud_run_v2_service" "api" {
  name     = "${local.name_prefix}-api"
  location = var.region

  template {
    scaling {
      min_instance_count = local.is_prod ? 1 : 0
      max_instance_count = local.is_prod ? 100 : 10
    }
  }
}
```

## Workspace Commands

| Command | Purpose |
|---------|---------|
| `terraform workspace list` | List all workspaces |
| `terraform workspace new dev` | Create workspace |
| `terraform workspace select prod` | Switch workspace |
| `terraform workspace show` | Show current workspace |
| `terraform workspace delete staging` | Delete workspace |

## GCS State Paths

```text
gs://my-project-terraform-state/
  terraform/state/
    default.tfstate              # "default" workspace
    env:/
      dev/
        default.tfstate          # "dev" workspace
      staging/
        default.tfstate          # "staging" workspace
      prod/
        default.tfstate          # "prod" workspace
```

## Per-Environment Variables

```hcl
# environments/dev.tfvars
project_id  = "my-project-dev"
region      = "us-central1"
min_scale   = 0
max_scale   = 5

# environments/prod.tfvars
project_id  = "my-project-prod"
region      = "us-central1"
min_scale   = 2
max_scale   = 100

# Usage:
# terraform workspace select dev
# terraform apply -var-file="environments/dev.tfvars"
```

## Workspace vs. Directory Separation

| Approach | Pros | Cons |
|----------|------|------|
| **Workspaces** | DRY, single codebase | Shared provider config |
| **Directories** | Full isolation | Code duplication |
| **Terragrunt** | DRY + isolation | Additional tooling |

## Conditional Resources

```hcl
# Only create monitoring in prod
resource "google_monitoring_alert_policy" "high_latency" {
  count = local.is_prod ? 1 : 0

  display_name = "High Latency Alert"
  project      = var.project_id

  conditions {
    display_name = "Latency > 1s"
    condition_threshold {
      filter          = "resource.type = \"cloud_run_revision\""
      comparison      = "COMPARISON_GT"
      threshold_value = 1000
      duration        = "60s"
    }
  }
}
```

## Common Mistakes

### Wrong

```hcl
# Using workspace name directly in bucket names (may conflict)
resource "google_storage_bucket" "data" {
  name = terraform.workspace  # Not globally unique
}
```

### Correct

```hcl
# Combine workspace with project for unique names
resource "google_storage_bucket" "data" {
  name = "${var.project_id}-${terraform.workspace}-data"
}
```

## Related

- [State](../concepts/state.md)
- [Variables](../concepts/variables.md)
- [Remote State](../patterns/remote-state.md)
