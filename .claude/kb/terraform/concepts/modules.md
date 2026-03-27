# Modules

> **Purpose**: Reusable, composable Terraform packages for GCP infrastructure
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Modules are containers for multiple resources that are used together. A module
consists of `.tf` files in a directory. Every Terraform configuration has at least
one module (the root module). Child modules enable code reuse, organization, and
encapsulation of infrastructure logic.

## The Pattern

```hcl
# Module definition: modules/cloud-run/main.tf
variable "service_name" {
  type        = string
  description = "Name of the Cloud Run service"
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "container_image" {
  type = string
}

resource "google_cloud_run_v2_service" "service" {
  name     = var.service_name
  location = var.region
  project  = var.project_id

  template {
    containers {
      image = var.container_image
    }
  }
}

output "service_url" {
  value = google_cloud_run_v2_service.service.uri
}

output "service_name" {
  value = google_cloud_run_v2_service.service.name
}
```

## Module Sources

| Source Type | Example | Use Case |
|-------------|---------|----------|
| Local path | `source = "./modules/cloud-run"` | Project-specific modules |
| Git repo | `source = "git::https://github.com/org/mod.git"` | Shared across repos |
| Terraform Registry | `source = "terraform-google-modules/pubsub/google"` | Google-maintained |
| GCS bucket | `source = "gcs::https://www.googleapis.com/..."` | Private modules |

## Calling Modules

```hcl
# Root module: main.tf
module "api_service" {
  source = "./modules/cloud-run"

  service_name    = "invoice-api"
  project_id      = var.project_id
  region          = var.region
  container_image = "gcr.io/${var.project_id}/api:latest"
}

module "worker_service" {
  source = "./modules/cloud-run"

  service_name    = "invoice-worker"
  project_id      = var.project_id
  region          = var.region
  container_image = "gcr.io/${var.project_id}/worker:latest"
}

# Reference module outputs
output "api_url" {
  value = module.api_service.service_url
}
```

## Module Best Practices

| Practice | Description |
|----------|-------------|
| Pin versions | `version = "~> 8.6"` for registry modules |
| Expose outputs | Return IDs, URLs, names for downstream use |
| No hardcoded providers | Configure providers in root module only |
| No hardcoded backends | Backends belong in root module only |
| Validate inputs | Use `validation` blocks on variables |
| Minimal interface | Expose only necessary variables |
| Use `for_each` over `count` | Better state addressing with map keys |
| Variable validation refs (1.9+) | Reference other vars in validation conditions |

## Variable Validation with References (Terraform 1.9+)

```hcl
variable "environment" {
  type = string
}

variable "min_instances" {
  type = number
  validation {
    condition     = var.environment == "prod" ? var.min_instances >= 2 : true
    error_message = "Production requires at least 2 instances."
  }
}
```

## HCP Terraform Stacks for Multi-Environment Modules

Stacks let you deploy the same module composition across environments:

```hcl
# deployment.tfdeploy.hcl
deployment "dev" {
  inputs = {
    environment = "dev"
    region      = "us-central1"
  }
}

deployment "prod" {
  inputs = {
    environment = "prod"
    region      = "us-central1"
  }
}
```

## Common Mistakes

### Wrong

```hcl
# Provider configured inside module
module "bad_example" {
  source = "./modules/storage"

  providers = {
    google = google.custom  # Avoid provider passthrough when possible
  }
}
```

### Correct

```hcl
# Provider configured in root, module inherits
provider "google" {
  project = var.project_id
  region  = var.region
}

module "storage" {
  source     = "./modules/storage"
  project_id = var.project_id
  region     = var.region
}
```

## Related

- [Resources](../concepts/resources.md)
- [Variables](../concepts/variables.md)
- [Cloud Run Module](../patterns/cloud-run-module.md)
- [Pub/Sub Module](../patterns/pubsub-module.md)
