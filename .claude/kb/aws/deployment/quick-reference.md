# AWS Deployment Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-02-17
> **Last Updated:** 2026-03-26

## SAM CLI Commands

| Command | Purpose | Key Flags |
|---------|---------|-----------|
| `sam init` | Scaffold new project | `--runtime python3.14 --app-template` |
| `sam build` | Build artifacts | `--use-container --parallel` |
| `sam deploy` | Deploy to AWS | `--guided --config-env dev` |
| `sam sync` | Hot-sync changes (Accelerate) | `--watch --stack-name` |
| `sam validate` | Validate template | `--lint` |
| `sam local invoke` | Run function locally | `-e event.json --env-vars` |
| `sam local start-api` | Local API Gateway | `--port 3000 --warm-containers` |
| `sam delete` | Tear down stack | `--stack-name --no-prompts` |
| `sam logs` | Tail CloudWatch logs | `--tail --stack-name` |

## SAM Accelerate (sam sync)

| What Changed | Deployment Path | Speed |
|-------------|----------------|-------|
| Lambda code only | Direct update (skip CloudFormation) | Seconds |
| Lambda layer code | Direct layer publish | Seconds |
| Infrastructure (new resources) | CloudFormation nested stacks | Minutes |
| Template config changes | CloudFormation update | Minutes |

## AWS CLI S3 Commands

| Command | Purpose | Key Flags |
|---------|---------|-----------|
| `aws s3 ls` | List buckets/objects | `--recursive --human-readable` |
| `aws s3 cp` | Copy files | `--recursive --exclude --include` |
| `aws s3 sync` | Sync directories | `--delete --exclude` |
| `aws s3 rm` | Delete objects | `--recursive` |
| `aws s3 mb` | Create bucket | `--region us-east-1` |
| `aws s3 presign` | Generate temp URL | `--expires-in 3600` |

## AWS CLI Essentials

| Command | Purpose |
|---------|---------|
| `aws configure` | Set credentials interactively |
| `aws configure list` | Show current config |
| `aws sts get-caller-identity` | Verify active identity |
| `aws lambda list-functions` | List deployed Lambdas |
| `aws cloudformation describe-stacks` | Check stack status |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| First-time deploy | `sam deploy --guided` |
| Repeat deploy (CI/CD) | `sam deploy --config-env prod` |
| Rapid iteration | `sam sync --watch` |
| Test single function | `sam local invoke FunctionName` |
| Test API endpoints | `sam local start-api` |
| Upload artifacts to S3 | `aws s3 sync ./dist s3://bucket/` |
| One-off file transfer | `aws s3 cp file.zip s3://bucket/` |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Deploy without building first | `sam build && sam deploy` |
| Hardcode credentials in templates | Use `AWS_PROFILE` or IAM roles |
| Skip `--guided` on first deploy | `sam deploy --guided` to generate samconfig.toml |
| Use `s3 cp` for large directory sync | Use `s3 sync` with `--delete` flag |
| Test in prod without local testing | `sam local invoke` before deploying |
| Forget `--use-container` for native deps | `sam build --use-container` for compiled packages |

## AWS Data Services Quick Reference

| Service | CLI Command | Purpose |
|---------|------------|---------|
| Glue | `aws glue start-job-run --job-name NAME` | Run Glue 5.0 ETL job |
| Glue | `aws glue get-job-runs --job-name NAME` | Check job run status |
| Redshift | `aws redshift-serverless get-workgroup --workgroup-name NAME` | Check Serverless workgroup |
| MWAA | `aws mwaa create-environment --name NAME` | Create Airflow 3.0 environment |
| MWAA | `aws mwaa create-cli-token --name NAME` | Get CLI token for Airflow API |

## Related Documentation

| Topic | Path |
|-------|------|
| SAM CLI commands | `concepts/sam-cli.md` |
| AWS CLI basics | `concepts/aws-cli.md` |
| Environment config | `concepts/environments.md` |
| Full Index | `index.md` |
