# Providers

> **Purpose**: GCP provider configuration, authentication methods, and version management
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

The Google Cloud provider (`hashicorp/google`) is the interface between Terraform
and GCP APIs. It handles authentication, project scoping, and region defaults.
The `google-beta` provider grants access to beta GCP features not yet in GA.

## The Pattern

```hcl
terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}
```

## Authentication Methods

| Method | Config | Best For |
|--------|--------|----------|
| ADC (gcloud CLI) | `gcloud auth application-default login` | Local dev |
| Service Account Key | `credentials = file("sa-key.json")` | Legacy systems |
| Workload Identity | `GOOGLE_IMPERSONATE_SERVICE_ACCOUNT` | CI/CD pipelines |
| Environment variable | `GOOGLE_CREDENTIALS` or `GOOGLE_APPLICATION_CREDENTIALS` | Containers |

## Recommended Authentication

```hcl
# Use ADC -- no explicit credentials in code
provider "google" {
  project = var.project_id
  region  = var.region
  # Authentication via: gcloud auth application-default login
}

# For CI/CD: impersonate a service account
provider "google" {
  project = var.project_id
  region  = var.region
  impersonate_service_account = var.terraform_sa_email
}
```

## Enabling GCP APIs

```hcl
# Enable required APIs before creating resources
resource "google_project_service" "apis" {
  for_each = toset([
    "run.googleapis.com",
    "pubsub.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "secretmanager.googleapis.com",
    "iam.googleapis.com",
  ])

  project = var.project_id
  service = each.value

  disable_dependent_services = false
  disable_on_destroy         = false
}
```

## Quick Reference

| Setting | Default | Notes |
|---------|---------|-------|
| `project` | None (required) | GCP project ID |
| `region` | None | Default region for regional resources |
| `zone` | None | Default zone for zonal resources |
| `credentials` | ADC | Path to SA key JSON (avoid if possible) |
| `impersonate_service_account` | None | SA email for impersonation |

## Common Mistakes

### Wrong

```hcl
# Hardcoded credentials in provider block
provider "google" {
  credentials = file("/path/to/service-account.json")
  project     = "my-project-12345"
  region      = "us-central1"
}
```

### Correct

```hcl
# Parameterized, credentials via ADC or environment
provider "google" {
  project = var.project_id
  region  = var.region
}
```

## Multiple Provider Aliases

```hcl
provider "google" {
  alias   = "us_central"
  project = var.project_id
  region  = "us-central1"
}

provider "google" {
  alias   = "europe_west"
  project = var.project_id
  region  = "europe-west1"
}

resource "google_storage_bucket" "eu_data" {
  provider = google.europe_west
  name     = "${var.project_id}-eu-data"
  location = "EU"
}
```

## Related

- [Resources](../concepts/resources.md)
- [State](../concepts/state.md)
- [Remote State](../patterns/remote-state.md)
