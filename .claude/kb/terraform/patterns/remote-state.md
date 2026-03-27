# Remote State with GCS Backend

> **Purpose**: Configure Terraform remote state storage in GCS with locking and cross-project references
> **MCP Validated**: 2026-02-17

## When to Use

- Team collaboration on shared infrastructure
- CI/CD pipelines requiring consistent state access
- Cross-project state references between Terraform configurations

## Implementation

```hcl
# Step 1: Create the state bucket (bootstrap -- run once)
# bootstrap/main.tf
provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "terraform_state" {
  name     = "${var.project_id}-terraform-state"
  location = var.region
  project  = var.project_id

  uniform_bucket_level_access = true
  force_destroy               = false

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      num_newer_versions = 5
      with_state         = "ARCHIVED"
    }
  }
}

resource "google_storage_bucket_iam_member" "state_admin" {
  bucket = google_storage_bucket.terraform_state.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${var.terraform_sa_email}"
}

output "state_bucket_name" {
  value = google_storage_bucket.terraform_state.name
}
```

```hcl
# Step 2: Configure backend in project root
# main.tf
terraform {
  required_version = ">= 1.5.0"

  backend "gcs" {
    bucket = "my-project-terraform-state"
    prefix = "terraform/infrastructure"
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}
```

```hcl
# Step 3: Initialize with dynamic backend config
# Use -backend-config for environment-specific values
# terraform init -backend-config="bucket=my-project-dev-tf-state"
# terraform init -backend-config="prefix=env/dev"
```

## Cross-Project State References

```hcl
# Reference state from another Terraform configuration
data "terraform_remote_state" "networking" {
  backend = "gcs"

  config = {
    bucket = "my-project-terraform-state"
    prefix = "terraform/networking"
  }
}

# Use outputs from remote state
resource "google_cloud_run_v2_service" "api" {
  name     = "api"
  location = var.region

  template {
    vpc_access {
      connector = data.terraform_remote_state.networking.outputs.vpc_connector_id
    }
  }
}
```

## Workspace-Aware State

```hcl
# GCS backend automatically separates workspace state
# Default workspace: <prefix>/default.tfstate
# Named workspace:   <prefix>/env:/<workspace>/default.tfstate

terraform {
  backend "gcs" {
    bucket = "my-project-terraform-state"
    prefix = "terraform/app"
  }
}

# State paths on GCS:
# terraform/app/default.tfstate           (default workspace)
# terraform/app/env:/dev/default.tfstate   (dev workspace)
# terraform/app/env:/prod/default.tfstate  (prod workspace)
```

## Configuration

| Setting | Required | Description |
|---------|----------|-------------|
| `bucket` | Yes | GCS bucket name |
| `prefix` | No | Path prefix in bucket |
| `encryption_key` | No | CSEK for state encryption |
| `impersonate_service_account` | No | SA for backend auth |

## State Security Checklist

| Item | Implementation |
|------|----------------|
| Versioning | `versioning { enabled = true }` on bucket |
| Access control | IAM restricted to Terraform SA |
| Encryption | Default Google-managed or CSEK |
| Lifecycle | Keep 5 versions, delete older |
| No git | Add `*.tfstate` to `.gitignore` |
| Locking | Automatic with GCS backend |

## Example: Multi-Layer State

```hcl
# Layer 1: Networking (terraform/networking/)
# Layer 2: Data (terraform/data/)
# Layer 3: Application (terraform/app/)

# In Layer 3, reference Layer 1 and Layer 2 outputs
data "terraform_remote_state" "networking" {
  backend = "gcs"
  config = {
    bucket = var.state_bucket
    prefix = "terraform/networking"
  }
}

data "terraform_remote_state" "data" {
  backend = "gcs"
  config = {
    bucket = var.state_bucket
    prefix = "terraform/data"
  }
}

module "api" {
  source          = "./modules/cloud-run"
  service_name    = "api"
  project_id      = var.project_id
  container_image = var.api_image
}
```

## See Also

- [State](../concepts/state.md)
- [Workspaces](../concepts/workspaces.md)
- [Providers](../concepts/providers.md)
