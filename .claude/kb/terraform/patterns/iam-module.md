# IAM Bindings Terraform Module

> **Purpose**: Reusable module for managing GCP service accounts, role bindings, and least-privilege access
> **MCP Validated**: 2026-02-17

## When to Use

- Creating service accounts for Cloud Run, Cloud Functions, or other workloads
- Assigning project-level or resource-level IAM roles
- Implementing least-privilege access patterns

## Implementation

```hcl
# modules/iam/variables.tf
variable "project_id" {
  type        = string
  description = "GCP project ID"
}

variable "service_accounts" {
  type = map(object({
    display_name = string
    description  = optional(string, "")
    roles        = list(string)
  }))
  default     = {}
  description = "Map of service accounts to create with their roles"
}

variable "project_iam_bindings" {
  type = map(object({
    role    = string
    members = list(string)
  }))
  default     = {}
  description = "Additional project-level IAM bindings"
}
```

```hcl
# modules/iam/main.tf

# Create service accounts
resource "google_service_account" "accounts" {
  for_each = var.service_accounts

  account_id   = each.key
  display_name = each.value.display_name
  description  = each.value.description
  project      = var.project_id
}

# Assign roles to service accounts
resource "google_project_iam_member" "sa_roles" {
  for_each = {
    for pair in flatten([
      for sa_key, sa in var.service_accounts : [
        for role in sa.roles : {
          key  = "${sa_key}-${replace(role, "/", "_")}"
          sa   = sa_key
          role = role
        }
      ]
    ]) : pair.key => pair
  }

  project = var.project_id
  role    = each.value.role
  member  = "serviceAccount:${google_service_account.accounts[each.value.sa].email}"
}

# Additional project IAM bindings
resource "google_project_iam_binding" "bindings" {
  for_each = var.project_iam_bindings

  project = var.project_id
  role    = each.value.role
  members = each.value.members
}
```

```hcl
# modules/iam/outputs.tf
output "service_account_emails" {
  value = {
    for k, v in google_service_account.accounts : k => v.email
  }
  description = "Map of service account key to email"
}

output "service_account_ids" {
  value = {
    for k, v in google_service_account.accounts : k => v.id
  }
}

output "service_account_names" {
  value = {
    for k, v in google_service_account.accounts : k => v.name
  }
}
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `service_accounts` | `{}` | Map of SA configs with roles |
| `project_iam_bindings` | `{}` | Extra project-level bindings |

## Common GCP Roles

| Role | Purpose |
|------|---------|
| `roles/run.invoker` | Invoke Cloud Run services |
| `roles/pubsub.publisher` | Publish to Pub/Sub topics |
| `roles/pubsub.subscriber` | Subscribe to Pub/Sub |
| `roles/storage.objectViewer` | Read GCS objects |
| `roles/storage.objectAdmin` | Full GCS object access |
| `roles/bigquery.dataEditor` | Read/write BigQuery data |
| `roles/bigquery.jobUser` | Run BigQuery jobs |
| `roles/secretmanager.secretAccessor` | Read secrets |
| `roles/logging.logWriter` | Write logs |

## Example Usage

```hcl
module "iam" {
  source     = "./modules/iam"
  project_id = var.project_id

  service_accounts = {
    "invoice-api-sa" = {
      display_name = "Invoice API Service Account"
      description  = "SA for the Invoice API Cloud Run service"
      roles = [
        "roles/pubsub.publisher",
        "roles/storage.objectViewer",
        "roles/bigquery.dataEditor",
        "roles/bigquery.jobUser",
        "roles/secretmanager.secretAccessor",
        "roles/logging.logWriter",
      ]
    }

    "invoice-worker-sa" = {
      display_name = "Invoice Worker Service Account"
      roles = [
        "roles/pubsub.subscriber",
        "roles/storage.objectAdmin",
        "roles/bigquery.dataEditor",
        "roles/bigquery.jobUser",
        "roles/logging.logWriter",
      ]
    }
  }
}

# Use the outputs in other modules
module "api_service" {
  source = "./modules/cloud-run"

  service_name          = "invoice-api"
  project_id            = var.project_id
  container_image       = var.api_image
  service_account_email = module.iam.service_account_emails["invoice-api-sa"]
}
```

## IAM Best Practices

| Practice | Description |
|----------|-------------|
| Least privilege | Grant only required roles per service |
| Use `iam_member` | Additive -- does not revoke other bindings |
| Avoid `iam_policy` | Authoritative -- removes all other bindings |
| Dedicated SAs | One service account per workload |
| No SA keys | Use Workload Identity or impersonation |

## See Also

- [Cloud Run Module](../patterns/cloud-run-module.md)
- [Providers](../concepts/providers.md)
- [Variables](../concepts/variables.md)
