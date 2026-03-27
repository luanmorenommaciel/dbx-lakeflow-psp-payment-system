# S3 Operations

> **Purpose**: AWS CLI S3 commands for artifact management, static hosting, and data operations
> **MCP Validated**: 2026-02-17

## When to Use

- Uploading deployment artifacts (Lambda layers, static assets)
- Syncing build output to S3 hosting buckets
- Managing data files for Lambda function processing
- Generating pre-signed URLs for temporary access

## Implementation

```bash
#!/bin/bash
# s3-deploy.sh - S3 artifact management workflow
set -euo pipefail

BUCKET="${1:?Usage: s3-deploy.sh <bucket> <env>}"
ENV="${2:-dev}"
DIST_DIR="./dist"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

echo "=== S3 Deployment to s3://${BUCKET} ==="

# Step 1: Sync build artifacts
echo "[1/4] Syncing ${DIST_DIR} to s3://${BUCKET}/releases/${TIMESTAMP}/"
aws s3 sync "${DIST_DIR}/" "s3://${BUCKET}/releases/${TIMESTAMP}/" \
  --exclude "*.pyc" \
  --exclude "__pycache__/*" \
  --exclude ".git/*" \
  --delete

# Step 2: Copy latest marker
echo "[2/4] Updating latest pointer..."
aws s3 cp \
  "s3://${BUCKET}/releases/${TIMESTAMP}/" \
  "s3://${BUCKET}/latest/" \
  --recursive

# Step 3: Verify upload
echo "[3/4] Verifying..."
aws s3 ls "s3://${BUCKET}/releases/${TIMESTAMP}/" \
  --recursive \
  --human-readable \
  --summarize

# Step 4: Generate access URL (if needed)
echo "[4/4] Generating pre-signed URL..."
aws s3 presign \
  "s3://${BUCKET}/releases/${TIMESTAMP}/artifact.zip" \
  --expires-in 3600
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `--delete` | false | Remove destination files not in source |
| `--exclude` | -- | Glob pattern to skip files |
| `--include` | -- | Glob pattern to include (after exclude) |
| `--recursive` | false | Apply to all files in prefix |
| `--acl` | -- | Access control (private, public-read) |
| `--storage-class` | STANDARD | STANDARD, INTELLIGENT_TIERING, GLACIER |
| `--expires-in` | 3600 | Pre-signed URL expiry in seconds |
| `--dryrun` | false | Preview without executing |
| `--content-type` | auto | Override MIME type |

## Core Commands

### s3 sync (Directory Synchronization)

```bash
# Sync local directory to S3
aws s3 sync ./build s3://my-bucket/assets/ --delete

# Sync S3 to local (download)
aws s3 sync s3://my-bucket/data/ ./local-data/

# Sync with filters
aws s3 sync ./src s3://my-bucket/code/ \
  --exclude "*" \
  --include "*.py" \
  --include "*.yaml"

# Dry run first
aws s3 sync ./build s3://my-bucket/assets/ --delete --dryrun

# Sync between S3 buckets
aws s3 sync s3://source-bucket/ s3://dest-bucket/ --delete
```

### s3 cp (Copy Files)

```bash
# Upload single file
aws s3 cp ./function.zip s3://my-bucket/artifacts/

# Download single file
aws s3 cp s3://my-bucket/config/settings.json ./local/

# Copy entire directory
aws s3 cp ./data/ s3://my-bucket/data/ --recursive

# Copy with metadata
aws s3 cp ./index.html s3://my-bucket/ \
  --content-type "text/html" \
  --cache-control "max-age=3600"

# Copy between buckets
aws s3 cp s3://source/file.csv s3://destination/file.csv
```

### s3 ls (List Objects)

```bash
# List all buckets
aws s3 ls

# List objects in bucket
aws s3 ls s3://my-bucket/

# List with prefix filter
aws s3 ls s3://my-bucket/releases/ --recursive

# Human-readable sizes with summary
aws s3 ls s3://my-bucket/ --recursive --human-readable --summarize
```

### s3 rm (Delete Objects)

```bash
# Delete single object
aws s3 rm s3://my-bucket/old-file.zip

# Delete all objects with prefix
aws s3 rm s3://my-bucket/temp/ --recursive

# Delete with dry run
aws s3 rm s3://my-bucket/logs/ --recursive --dryrun
```

### s3 presign (Temporary URLs)

```bash
# Generate URL valid for 1 hour
aws s3 presign s3://my-bucket/report.pdf --expires-in 3600

# Generate URL valid for 7 days (max)
aws s3 presign s3://my-bucket/archive.zip --expires-in 604800
```

## Common Mistakes

### Wrong

```bash
# Using cp for large directory sync (copies everything every time)
aws s3 cp ./build/ s3://my-bucket/ --recursive

# Forgetting --delete on sync (orphan files remain)
aws s3 sync ./build/ s3://my-bucket/

# No dry run before destructive sync
aws s3 sync ./empty-dir/ s3://my-bucket/important/ --delete
```

### Correct

```bash
# Use sync for directories (only transfers changed files)
aws s3 sync ./build/ s3://my-bucket/ --delete

# Always dry run destructive operations first
aws s3 sync ./build/ s3://my-bucket/ --delete --dryrun
# Review output, then run without --dryrun

# Use cp only for single files
aws s3 cp ./artifact.zip s3://my-bucket/releases/
```

## Example Usage

```bash
# Deploy static site
aws s3 sync ./public/ s3://my-website-bucket/ \
  --delete --cache-control "max-age=86400" --exclude "*.map"

# Backup S3 data locally
aws s3 sync s3://prod-data/ ./backups/prod/ --exclude "*.tmp"
```

## See Also

- [AWS CLI](../concepts/aws-cli.md)
- [SAM Deploy](../patterns/sam-deploy.md)
- [Environments](../concepts/environments.md)
