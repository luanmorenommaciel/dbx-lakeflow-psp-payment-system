> **MCP Validated:** 2026-02-17

# Power BI REST API

> **Purpose**: Patterns for Power BI REST API operations -- dataset refresh, report export, permission management, and embedding

## When to Use

- Automating scheduled and on-demand dataset refreshes
- Exporting Power BI reports to PDF, PNG, or PPTX programmatically
- Managing workspace and dataset permissions via scripts
- Embedding Power BI reports in custom applications

## Implementation

### Dataset Refresh

```python
"""Trigger and monitor Power BI dataset refreshes."""
import requests
import time
from azure.identity import ClientSecretCredential

BASE_URL = "https://api.powerbi.com/v1.0/myorg"

def get_headers(credential) -> dict:
    token = credential.get_token("https://analysis.windows.net/powerbi/api/.default")
    return {"Authorization": f"Bearer {token.token}"}

def refresh_dataset(
    credential, group_id: str, dataset_id: str,
    notify_option: str = "NoNotification",
) -> None:
    """Trigger a dataset refresh."""
    resp = requests.post(
        f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes",
        headers=get_headers(credential),
        json={"notifyOption": notify_option},
    )
    resp.raise_for_status()

def get_refresh_history(credential, group_id: str, dataset_id: str) -> list:
    """Get refresh history for a dataset."""
    resp = requests.get(
        f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/refreshes",
        headers=get_headers(credential),
    )
    resp.raise_for_status()
    return resp.json()["value"]

def wait_for_refresh(
    credential, group_id: str, dataset_id: str, timeout: int = 600,
) -> dict:
    """Poll until the latest refresh completes."""
    start = time.time()
    while time.time() - start < timeout:
        history = get_refresh_history(credential, group_id, dataset_id)
        latest = history[0]
        if latest["status"] != "Unknown":  # Unknown = in progress
            return latest
        time.sleep(15)
    raise TimeoutError("Refresh did not complete")
```

### Report Export

```python
"""Export Power BI reports to file formats."""

def export_report(
    credential, group_id: str, report_id: str,
    format: str = "PDF",  # PDF, PPTX, PNG
    pages: list[str] | None = None,
) -> bytes:
    """Export a report and return the file bytes."""
    headers = get_headers(credential)

    # Step 1: Initiate export
    payload = {"format": format}
    if pages:
        payload["powerBIReportConfiguration"] = {
            "pages": [{"pageName": p} for p in pages]
        }
    resp = requests.post(
        f"{BASE_URL}/groups/{group_id}/reports/{report_id}/exports",
        headers=headers,
        json=payload,
    )
    resp.raise_for_status()
    export_id = resp.json()["id"]

    # Step 2: Poll until export completes
    while True:
        status_resp = requests.get(
            f"{BASE_URL}/groups/{group_id}/reports/{report_id}"
            f"/exports/{export_id}",
            headers=headers,
        )
        status = status_resp.json()
        if status["status"] == "Succeeded":
            break
        if status["status"] == "Failed":
            raise RuntimeError(f"Export failed: {status}")
        time.sleep(5)

    # Step 3: Download the file
    file_resp = requests.get(
        f"{BASE_URL}/groups/{group_id}/reports/{report_id}"
        f"/exports/{export_id}/file",
        headers=headers,
    )
    return file_resp.content

# Usage
pdf_bytes = export_report(credential, group_id, report_id, format="PDF")
with open("report.pdf", "wb") as f:
    f.write(pdf_bytes)
```

### Permission Management

```python
"""Manage Power BI workspace and dataset permissions."""

def add_workspace_user(
    credential, group_id: str, email: str, access_right: str,
) -> None:
    """Add a user to a workspace. access_right: Admin, Member, Contributor, Viewer."""
    resp = requests.post(
        f"{BASE_URL}/groups/{group_id}/users",
        headers=get_headers(credential),
        json={
            "emailAddress": email,
            "groupUserAccessRight": access_right,
        },
    )
    resp.raise_for_status()

def add_dataset_permission(
    credential, group_id: str, dataset_id: str,
    principal_id: str, access_right: str = "Read",
) -> None:
    """Grant dataset-level permission to a principal."""
    resp = requests.post(
        f"{BASE_URL}/groups/{group_id}/datasets/{dataset_id}/users",
        headers=get_headers(credential),
        json={
            "identifier": principal_id,
            "principalType": "User",
            "datasetUserAccessRight": access_right,
        },
    )
    resp.raise_for_status()
```

### Report Embedding

```python
"""Generate embed tokens for Power BI reports."""

def get_embed_token(
    credential, group_id: str, report_id: str, dataset_id: str,
) -> dict:
    """Generate an embed token for app-owns-data embedding."""
    resp = requests.post(
        f"{BASE_URL}/groups/{group_id}/reports/{report_id}/GenerateToken",
        headers=get_headers(credential),
        json={
            "accessLevel": "View",
            "datasetId": dataset_id,
        },
    )
    resp.raise_for_status()
    data = resp.json()
    return {
        "token": data["token"],
        "expiration": data["expiration"],
        "embed_url": f"https://app.powerbi.com/reportEmbed"
                     f"?reportId={report_id}&groupId={group_id}",
    }
```

## Configuration

| Setting | Value | Description |
|---------|-------|-------------|
| API base URL | `https://api.powerbi.com/v1.0/myorg` | Power BI REST endpoint |
| Token scope | `https://analysis.windows.net/powerbi/api/.default` | OAuth2 scope |
| Export formats | PDF, PPTX, PNG, CSV | Supported export types |
| Refresh concurrency | 5 parallel per dataset | Enhanced refresh API |
| Max exports/hour | 50 per user | Export rate limit |

## Related

- [REST API Fundamentals](../concepts/rest-api.md)
- [Fabric REST API v1](../concepts/fabric-rest-api.md)
- [SDK Automation](sdk-automation.md)
- [Python SDK Automation](python-sdk-automation.md)
