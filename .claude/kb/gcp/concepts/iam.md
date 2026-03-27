# IAM (Identity and Access Management)

> **Purpose**: Access control for GCP resources using service accounts, roles, and policies
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

GCP IAM controls who (identity) has what access (role) to which resource. In serverless
data pipelines, each Cloud Run service runs as a dedicated service account with the minimum
permissions required (least privilege). IAM bindings connect service accounts to predefined
roles, granting access to specific GCP resources like buckets, topics, and datasets.

## The Pattern

```python
from google.cloud import iam_credentials_v1

def get_service_account_token(
    service_account_email: str,
    scopes: list[str] = None
) -> str:
    """Generate an access token for a service account."""
    client = iam_credentials_v1.IAMCredentialsClient()
    scopes = scopes or ["https://www.googleapis.com/auth/cloud-platform"]
    response = client.generate_access_token(
        request={
            "name": f"projects/-/serviceAccounts/{service_account_email}",
            "scope": scopes,
        }
    )
    return response.access_token
```

## Key Roles for Data Pipelines

| Role | Grants | Assign To |
|------|--------|-----------|
| `roles/run.invoker` | Invoke Cloud Run service | Pub/Sub SA, Scheduler SA |
| `roles/pubsub.publisher` | Publish to topics | Cloud Run SA, GCS notification |
| `roles/pubsub.subscriber` | Pull from subscriptions | Cloud Run SA |
| `roles/storage.objectViewer` | Read GCS objects | Cloud Run SA |
| `roles/storage.objectCreator` | Write GCS objects | Cloud Run SA |
| `roles/bigquery.dataEditor` | Insert/update BQ tables | Cloud Run SA |
| `roles/bigquery.jobUser` | Run BQ queries | Cloud Run SA |
| `roles/secretmanager.secretAccessor` | Read secrets | Cloud Run SA |

## Quick Reference

| Input | Output | Notes |
|-------|--------|-------|
| `gcloud iam service-accounts create NAME` | Service account | One per Cloud Run service |
| `gcloud projects add-iam-policy-binding` | Policy binding | Project-level role grant |
| `gcloud run services add-iam-policy-binding` | Service binding | Service-level invocation |
| Default Compute SA | Auto-assigned | Too permissive for production |

## Common Mistakes

### Wrong

```bash
# Granting overly broad roles
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:my-sa@my-project.iam.gserviceaccount.com" \
    --role="roles/editor"
# Using default compute service account in production
```

### Correct

```bash
# Granting least-privilege roles
gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:pipeline-sa@my-project.iam.gserviceaccount.com" \
    --role="roles/storage.objectViewer"

gcloud projects add-iam-policy-binding my-project \
    --member="serviceAccount:pipeline-sa@my-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.dataEditor"

# Deploy Cloud Run with dedicated service account
gcloud run deploy my-service \
    --service-account=pipeline-sa@my-project.iam.gserviceaccount.com
```

## Service Account Naming Convention

```text
{service-name}-sa@{project-id}.iam.gserviceaccount.com

Examples:
  data-ingestion-sa@my-project.iam.gserviceaccount.com
  data-processor-sa@my-project.iam.gserviceaccount.com
  bq-loader-sa@my-project.iam.gserviceaccount.com
```

## Related

- [Cloud Run](../concepts/cloud-run.md) - Runs as service account
- [Secret Manager](../concepts/secret-manager.md) - Requires secretAccessor role
- [GCS](../concepts/gcs.md) - Requires storage roles
- [BigQuery](../concepts/bigquery.md) - Requires bigquery roles
