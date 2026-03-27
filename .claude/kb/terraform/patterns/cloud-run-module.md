# Cloud Run Terraform Module

> **Purpose**: Reusable module for deploying Cloud Run services on GCP with IAM, scaling, and env config
> **MCP Validated**: 2026-02-17

## When to Use

- Deploying containerized APIs or workers to Cloud Run
- Standardizing Cloud Run configuration across services
- Managing scaling, IAM, and environment variables declaratively

## Implementation

```hcl
# modules/cloud-run/variables.tf
variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "region" {
  type        = string
  default     = "us-central1"
  description = "GCP region for the Cloud Run service"
}

variable "service_name" {
  type        = string
  description = "Name of the Cloud Run service"
}

variable "container_image" {
  type        = string
  description = "Container image to deploy (gcr.io/... or docker.io/...)"
}

variable "cpu" {
  type    = string
  default = "1000m"
}

variable "memory" {
  type    = string
  default = "512Mi"
}

variable "min_scale" {
  type    = number
  default = 0
}

variable "max_scale" {
  type    = number
  default = 10
}

variable "env_vars" {
  type        = map(string)
  default     = {}
  description = "Environment variables as key-value pairs"
}

variable "service_account_email" {
  type        = string
  default     = null
  description = "Custom service account email"
}

variable "ingress" {
  type    = string
  default = "INGRESS_TRAFFIC_ALL"
}

variable "allow_unauthenticated" {
  type    = bool
  default = false
}

variable "labels" {
  type    = map(string)
  default = {}
}
```

```hcl
# modules/cloud-run/main.tf
resource "google_cloud_run_v2_service" "service" {
  name     = var.service_name
  location = var.region
  project  = var.project_id
  ingress  = var.ingress
  labels   = var.labels

  template {
    service_account = var.service_account_email

    containers {
      image = var.container_image

      resources {
        limits = {
          cpu    = var.cpu
          memory = var.memory
        }
      }

      dynamic "env" {
        for_each = var.env_vars
        content {
          name  = env.key
          value = env.value
        }
      }
    }

    scaling {
      min_instance_count = var.min_scale
      max_instance_count = var.max_scale
    }
  }
}

resource "google_cloud_run_v2_service_iam_member" "public" {
  count    = var.allow_unauthenticated ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.service.name
  role     = "roles/run.invoker"
  member   = "allUsers"
}
```

```hcl
# modules/cloud-run/outputs.tf
output "service_url" {
  value       = google_cloud_run_v2_service.service.uri
  description = "URL of the deployed Cloud Run service"
}

output "service_name" {
  value = google_cloud_run_v2_service.service.name
}

output "service_id" {
  value = google_cloud_run_v2_service.service.id
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `cpu` | `1000m` | CPU allocation per instance |
| `memory` | `512Mi` | Memory allocation per instance |
| `min_scale` | `0` | Minimum instances (0 = scale to zero) |
| `max_scale` | `10` | Maximum concurrent instances |
| `ingress` | `INGRESS_TRAFFIC_ALL` | Ingress traffic filter |
| `allow_unauthenticated` | `false` | Public access without auth |

## Example Usage

```hcl
module "invoice_api" {
  source = "./modules/cloud-run"

  project_id      = var.project_id
  region          = "us-central1"
  service_name    = "${var.project_id}-invoice-api"
  container_image = "gcr.io/${var.project_id}/invoice-api:v1.2.0"
  cpu             = "2000m"
  memory          = "1Gi"
  min_scale       = 1
  max_scale       = 50

  env_vars = {
    PROJECT_ID  = var.project_id
    ENVIRONMENT = var.environment
    LOG_LEVEL   = "info"
  }

  service_account_email = google_service_account.api_sa.email
  allow_unauthenticated = false

  labels = {
    app         = "invoice-api"
    environment = var.environment
  }
}
```

## See Also

- [Pub/Sub Module](../patterns/pubsub-module.md)
- [IAM Module](../patterns/iam-module.md)
- [Resources](../concepts/resources.md)
- [Modules](../concepts/modules.md)
