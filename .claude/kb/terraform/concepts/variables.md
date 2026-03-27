# Variables, Locals, and Outputs

> **Purpose**: Parameterization, computed values, and exported attributes in Terraform
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Variables make Terraform configurations flexible and reusable. Input variables
accept external values, locals compute intermediate values, and outputs expose
attributes for other modules or the CLI. Together they form the data flow of
any Terraform project.

## Input Variables

```hcl
variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  description = "GCP region for resources"
  default     = "us-central1"
}

variable "environment" {
  type        = string
  description = "Deployment environment"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "services" {
  type = map(object({
    image       = string
    cpu         = optional(string, "1000m")
    memory      = optional(string, "512Mi")
    min_scale   = optional(number, 0)
    max_scale   = optional(number, 10)
  }))
  description = "Map of Cloud Run services to deploy"
}
```

## Setting Variables

| Method | Precedence | Example |
|--------|------------|---------|
| Default value | Lowest | `default = "us-central1"` |
| Environment var | Low | `export TF_VAR_project_id="my-proj"` |
| `terraform.tfvars` | Medium | `project_id = "my-proj"` |
| `*.auto.tfvars` | Medium | Auto-loaded from files |
| `-var` flag | High | `terraform apply -var="region=eu"` |
| `-var-file` flag | Highest | `terraform apply -var-file="prod.tfvars"` |

## Locals

```hcl
locals {
  # Computed naming convention
  name_prefix = "${var.project_id}-${var.environment}"

  # Common labels applied to all resources
  common_labels = {
    project     = var.project_id
    environment = var.environment
    managed_by  = "terraform"
  }

  # Conditional logic
  is_prod    = var.environment == "prod"
  min_scale  = local.is_prod ? 1 : 0
  max_scale  = local.is_prod ? 100 : 10
}

resource "google_cloud_run_v2_service" "api" {
  name     = "${local.name_prefix}-api"
  location = var.region
  labels   = local.common_labels

  template {
    scaling {
      min_instance_count = local.min_scale
      max_instance_count = local.max_scale
    }
  }
}
```

## Outputs

```hcl
output "service_url" {
  value       = google_cloud_run_v2_service.api.uri
  description = "URL of the deployed Cloud Run service"
}

output "service_account_email" {
  value       = google_service_account.sa.email
  description = "Service account email"
  sensitive   = true
}

# Accessing module outputs
output "api_endpoint" {
  value = module.api_service.service_url
}
```

## Common Mistakes

### Wrong

```hcl
# No type, no description, no validation
variable "env" {}
```

### Correct

```hcl
variable "environment" {
  type        = string
  description = "Target deployment environment"
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Must be dev, staging, or prod."
  }
}
```

## Related

- [Resources](../concepts/resources.md)
- [Modules](../concepts/modules.md)
- [Workspaces](../concepts/workspaces.md)
