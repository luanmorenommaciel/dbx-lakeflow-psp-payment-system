# Terraform for GCP Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-02-17
> **Last Updated:** 2026-03-26

## Essential Commands

| Command | Purpose | Notes |
|---------|---------|-------|
| `terraform init` | Initialize working directory | Downloads providers and modules |
| `terraform plan` | Preview changes | Always run before apply |
| `terraform apply` | Apply changes | Use `-auto-approve` only in CI |
| `terraform destroy` | Remove all resources | Requires confirmation |
| `terraform fmt` | Format HCL files | Use `-recursive` for all dirs |
| `terraform validate` | Syntax validation | Does not check provider APIs |
| `terraform import` | Import existing resources | `terraform import RESOURCE ID` |
| `terraform state list` | List state resources | Useful for debugging |
| `terraform test` | Run integration tests | TF 1.11+ with improved assertions |

## GCP Resource Types

| Resource | Terraform Type | Key Arguments |
|----------|---------------|---------------|
| Cloud Run | `google_cloud_run_v2_service` | `location`, `template`, `ingress` |
| Pub/Sub Topic | `google_pubsub_topic` | `name`, `message_retention_duration` |
| Pub/Sub Sub | `google_pubsub_subscription` | `topic`, `ack_deadline_seconds` |
| GCS Bucket | `google_storage_bucket` | `name`, `location`, `lifecycle_rule` |
| BigQuery Dataset | `google_bigquery_dataset` | `dataset_id`, `location` |
| BigQuery Table | `google_bigquery_table` | `dataset_id`, `table_id`, `schema` |
| Service Account | `google_service_account` | `account_id`, `display_name` |
| IAM Binding | `google_project_iam_binding` | `project`, `role`, `members` |

## Variable Types

| Type | Example | Use Case |
|------|---------|----------|
| `string` | `"us-central1"` | Single values |
| `number` | `3` | Counts, sizes |
| `bool` | `true` | Feature flags |
| `list(string)` | `["a","b"]` | Multiple values |
| `map(string)` | `{key = "val"}` | Key-value pairs |
| `object({...})` | Complex structs | Module inputs |

## Ephemeral Resources and Write-Only Arguments (1.10+/1.11+)

| Feature | Version | Purpose |
|---------|---------|---------|
| `ephemeral` block | 1.10+ | Temporary resources not stored in state/plan |
| Ephemeral variables | 1.10+ | `ephemeral = true` on input variables |
| Write-only arguments | 1.11+ | `_wo` suffix args on managed resources |
| `for_each` on ephemeral | 1.10+ | Multiple ephemeral resources from collections |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Single environment | Direct resource blocks |
| Multi-environment | Workspaces or Terragrunt |
| Reusable infra | Modules with variables |
| Team collaboration | Remote state with locking |
| Secret management | `google_secret_manager_secret` |
| Secrets out of state | Ephemeral resources (1.10+) + write-only args (1.11+) |
| Multi-env deployment | HCP Terraform Stacks |
| Open-source IaC | OpenTofu (MPL 2.0, drop-in replacement) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Hardcode project IDs | Use `var.project_id` |
| Store state locally | Use GCS backend with locking |
| Skip `terraform plan` | Always plan before apply |
| Use `count` for complex logic | Use `for_each` with maps |
| Commit `.terraform/` | Add to `.gitignore` |
| Store secrets in state with `sensitive` | Use ephemeral resources + write-only args (1.11+) |
| Use BSL Terraform if licensing concerns | Evaluate OpenTofu (MPL 2.0, drop-in compatible) |

## Related Documentation

| Topic | Path |
|-------|------|
| Provider Setup | `concepts/providers.md` |
| Resource Basics | `concepts/resources.md` |
| Module Patterns | `concepts/modules.md` |
| Full Index | `index.md` |
