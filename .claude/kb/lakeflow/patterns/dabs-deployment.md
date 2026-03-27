# DABs Deployment Patterns

> **MCP Validated**: 2025-01-19
> **Source**: https://docs.databricks.com/aws/en/dev-tools/bundles

## Databricks Asset Bundles (DABs)

DABs provide declarative deployment of Databricks resources including Lakeflow pipelines.

## Project Structure

```text
kurv-edp-lakeflow/
├── databricks.yml           # Main bundle config
├── resources/
│   ├── mdi_pipeline.yml     # MDI Lakeflow pipeline
│   ├── tddf_pipeline.yml    # TDDF Lakeflow pipeline
│   ├── adf_pipeline.yml     # ADF Lakeflow pipeline
│   └── d256_pipeline.yml    # D256 Lakeflow pipeline
├── src/
│   ├── mdi/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   ├── tddf/
│   │   ├── bronze.py
│   │   ├── silver.py
│   │   └── gold.py
│   └── shared/
│       └── expectations.py
└── tests/
    └── test_pipelines.py
```

## Main Bundle Configuration

```yaml
bundle:
  name: kurv-edp-lakeflow

variables:
  catalog:
    default: kurv_edp
  schema:
    default: lakeflow
  landing_bucket:
    default: kurv-edp-landing
  stage_bucket:
    default: kurv-edp-stage

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    variables:
      catalog: kurv_edp_dev
      schema: lakeflow_dev
      landing_bucket: kurv-edp-landing-dev
      stage_bucket: kurv-edp-stage-dev
    workspace:
      host: https://dbc-xxxxx.cloud.databricks.com

  prd:
    mode: production
    variables:
      catalog: kurv_edp
      schema: lakeflow
      landing_bucket: kurv-edp-landing-prd
      stage_bucket: kurv-edp-stage-prd
    workspace:
      host: https://dbc-xxxxx.cloud.databricks.com
    run_as:
      service_principal_name: kurv-edp-service-principal
```

## Pipeline Resource Configuration

### MDI Pipeline (`resources/mdi_pipeline.yml`)

```yaml
resources:
  pipelines:
    mdi_pipeline:
      name: "kurv-edp-mdi-${bundle.target}"
      catalog: "${var.catalog}"
      target: "bronze"                # Default schema; fully qualified names override
      serverless: true                # Serverless and clusters: are mutually exclusive
      channel: "CURRENT"
      edition: "ADVANCED"             # Required for EXPECT/DROP/FAIL expectations
      photon: true
      continuous: false

      libraries:
        - notebook:
            path: ../src/mdi/bronze_mdi.sql
        - notebook:
            path: ../src/mdi/silver_mdi.sql
        - notebook:
            path: ../src/mdi/gold_mdi.sql

      configuration:
        stage_bucket: "${var.stage_bucket}"
        catalog: "${var.catalog}"
```

### TDDF Pipeline (Multiple Tables)

```yaml
resources:
  pipelines:
    tddf_pipeline:
      name: "kurv-edp-tddf-${bundle.target}"
      catalog: "${var.catalog}"
      target: "bronze"
      serverless: true
      channel: "CURRENT"
      edition: "ADVANCED"
      photon: true
      continuous: false

      libraries:
        - notebook:
            path: ../src/tddf/bronze_tddf.sql
        - notebook:
            path: ../src/tddf/silver_tddf.sql
        - notebook:
            path: ../src/tddf/gold_tddf.sql

      configuration:
        stage_bucket: "${var.stage_bucket}"
        catalog: "${var.catalog}"

      notifications:
        - email_recipients:
            - data-team@kurvpay.com
          alerts:
            - on_failure
            - on_flow_failure
```

## CLI Commands

### Deploy to Dev

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

### Deploy to Production

```bash
databricks bundle deploy --target prd
```

### Run Pipeline

```bash
databricks bundle run mdi_pipeline --target dev
databricks bundle run mdi_pipeline --target dev --refresh-all
```

### Destroy Resources

```bash
databricks bundle destroy --target dev
```

## Parameterization Patterns

### Access Parameters in Pipeline Code

```python
import dlt

stage_bucket = spark.conf.get("stage_bucket")
landing_bucket = spark.conf.get("landing_bucket")

@dlt.table(name="mdi_bronze")
def mdi_bronze():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(f"s3://{stage_bucket}/mdi/")
    )
```

### Environment-Specific Logic

```python
import dlt

is_dev = spark.conf.get("pipelines.development", "false") == "true"

@dlt.table(name="mdi_silver")
@dlt.expect_or_drop("valid_merchant", "merchant_number IS NOT NULL")
def mdi_silver():
    df = dlt.read_stream("mdi_bronze")

    if is_dev:
        df = df.limit(10000)

    return df
```

## Unity Catalog Integration

```yaml
resources:
  pipelines:
    mdi_pipeline:
      catalog: "${var.catalog}"
      target: "${var.schema}"

      permissions:
        - level: CAN_VIEW
          group_name: data-consumers
        - level: CAN_RUN
          group_name: data-engineers
        - level: CAN_MANAGE
          service_principal_name: kurv-edp-admin
```

## Validation Checklist

Before deploying:

```bash
databricks bundle validate
databricks bundle validate --target prd
```

| Check | Command |
|-------|---------|
| Syntax valid | `databricks bundle validate` |
| Variables resolved | Check output for unresolved `${var.*}` |
| Permissions correct | Review `permissions` section |
| Cluster config valid | Check `clusters` autoscale settings |

## Related

- [Gold Aggregation](gold-aggregation.md) - Pipeline output
- [Pipeline Configuration](../reference/pipeline-configuration.md) - Advanced settings
- [Unity Catalog](../reference/unity-catalog.md) - Catalog integration
