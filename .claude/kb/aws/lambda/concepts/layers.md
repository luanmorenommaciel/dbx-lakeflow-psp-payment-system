# Lambda Layers

> **Purpose**: Shared dependency packages for Lambda functions (pandas, pyarrow, Powertools)
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

Lambda layers are ZIP archives containing libraries, custom runtimes, or other dependencies.
Layers reduce deployment package size, promote code reuse across functions, and separate
business logic from dependencies. A function can use up to 5 layers. The total unzipped
size of function code plus all layers cannot exceed 250 MB.

## The Pattern

```yaml
# SAM template using managed layers (Python 3.14)
Resources:
  ProcessorFunction:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.lambda_handler
      Runtime: python3.14
      MemorySize: 512  # Minimum for pandas/pyarrow
      Layers:
        # AWS SDK for pandas (includes pandas, pyarrow, numpy, boto3)
        - !Sub arn:aws:lambda:${AWS::Region}:336392948345:layer:AWSSDKPandas-Python314:1
        # Powertools for AWS Lambda v3
        - !Sub arn:aws:lambda:${AWS::Region}:017000801446:layer:AWSLambdaPowertoolsPythonV3-python314-x86_64:1
```

## AWS-Managed Layer ARNs

### Python 3.14 (Latest)

| Layer | Python 3.14 ARN | Size |
|-------|------------------|------|
| AWS SDK for pandas | `336392948345:layer:AWSSDKPandas-Python314:*` | ~160 MB |
| Powertools v3 (x86) | `017000801446:layer:AWSLambdaPowertoolsPythonV3-python314-x86_64:*` | ~15 MB |
| Powertools v3 (arm64) | `017000801446:layer:AWSLambdaPowertoolsPythonV3-python314-arm64:*` | ~15 MB |

### Python 3.12 (Stable)

| Layer | Python 3.12 ARN | Size |
|-------|------------------|------|
| AWS SDK for pandas | `336392948345:layer:AWSSDKPandas-Python312:15` | ~160 MB |
| Powertools v3 (x86) | `017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-x86_64:7` | ~15 MB |
| Powertools v3 (arm64) | `017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-arm64:7` | ~15 MB |

Note: Layer version numbers increment. Check current versions at:
- [AWS SDK for pandas releases](https://github.com/aws/aws-sdk-pandas/releases)
- [Powertools releases](https://github.com/aws-powertools/powertools-lambda-python/releases)

## Quick Reference

| Constraint | Limit |
|------------|-------|
| Max layers per function | 5 |
| Max unzipped total (code + layers) | 250 MB |
| Max zipped layer size | 50 MB (direct upload) |
| Layer extraction path | `/opt/` |
| Python packages path | `/opt/python/` |

## Building Custom Layers

```bash
# Create layer directory structure
mkdir -p layer/python

# Install packages for Lambda's Linux environment
pip install pandas pyarrow \
  --target layer/python \
  --platform manylinux2014_x86_64 \
  --only-binary=:all: \
  --python-version 3.12

# Package the layer
cd layer && zip -r9 ../pandas-layer.zip python/

# Publish layer
aws lambda publish-layer-version \
  --layer-name pandas-pyarrow \
  --compatible-runtimes python3.12 \
  --zip-file fileb://pandas-layer.zip
```

## SAM Custom Layer Definition

```yaml
Resources:
  SharedDependenciesLayer:
    Type: AWS::Serverless::LayerVersion
    Properties:
      LayerName: shared-dependencies
      Description: Common Python packages
      ContentUri: layers/shared/
      CompatibleRuntimes:
        - python3.12
      RetentionPolicy: Retain
    Metadata:
      BuildMethod: python3.12

  MyFunction:
    Type: AWS::Serverless::Function
    Properties:
      Layers:
        - !Ref SharedDependenciesLayer
```

## Common Mistakes

### Wrong (bundling pandas in function code)

```text
src/
  app.py
  pandas/          # 150 MB+ in function package
  pyarrow/         # Combined > 250 MB limit!
  requirements.txt
```

### Correct (using layer for heavy dependencies)

```text
src/
  app.py           # Only business logic (~5 KB)
  requirements.txt # Only lightweight deps
# pandas/pyarrow provided by managed layer
```

## Memory Considerations

When using pandas/pyarrow layers, set `MemorySize: 512` minimum.
The AWS SDK for pandas layer imports can take 2-3 seconds on cold start.
For faster cold starts, consider using only `pyarrow` without full pandas.

## Related

- [SAM Templates](../concepts/sam-templates.md)
- [Parquet Output](../patterns/parquet-output.md)
- [Powertools Logging](../patterns/powertools-logging.md)
