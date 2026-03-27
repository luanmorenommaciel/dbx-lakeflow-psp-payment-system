# Training Data Pipelines

> **Purpose**: ML training data management -- DVC versioning, annotation workflows, dataset splits, augmentation, bias detection, and reproducibility
> **MCP Validated**: 2026-03-26

## When to Use

- Versioning large datasets alongside code in Git
- Managing human annotation workflows for labeled data
- Need reproducible train/val/test splits with stratification
- Augmenting text data for low-resource NLP tasks
- Auditing datasets for demographic bias before model training

## Implementation

```python
"""Training data pipeline: versioning, splitting, augmentation, bias detection."""

# --- 1. DVC Data Versioning (CLI + Python) ---
# Initialize DVC in a Git repo
# $ dvc init
# $ dvc add data/training_dataset.parquet
# $ git add data/training_dataset.parquet.dvc data/.gitignore
# $ git commit -m "Track training dataset v1"
# $ dvc push  # Push data to remote storage (S3, GCS, Azure)

# dvc.yaml pipeline definition
DVC_PIPELINE = """
stages:
  prepare:
    cmd: python src/prepare.py
    deps:
      - src/prepare.py
      - data/raw/
    outs:
      - data/processed/
    params:
      - prepare.split_ratio
      - prepare.random_seed
  train:
    cmd: python src/train.py
    deps:
      - src/train.py
      - data/processed/
    outs:
      - models/
    metrics:
      - metrics.json:
          cache: false
"""

# --- 2. Label Studio Integration ---
from label_studio_sdk import Client

ls_client = Client(url="http://localhost:8080", api_key="your-api-key")
project = ls_client.create_project(
    title="NER Annotation - v2",
    label_config="""
    <View>
      <Labels name="label" toName="text">
        <Label value="PER" background="red"/>
        <Label value="ORG" background="blue"/>
        <Label value="LOC" background="green"/>
      </Labels>
      <Text name="text" value="$text"/>
    </View>
    """,
)
# Import tasks from dataset
project.import_tasks([{"text": row["content"]} for row in unlabeled_data])

# Export completed annotations
annotations = project.export_tasks(export_type="JSON")

# --- 3. Dataset Splits with Stratification ---
from sklearn.model_selection import train_test_split
import pandas as pd

df = pd.read_parquet("data/processed/labeled_data.parquet")

# Stratified split preserving label distribution
train_df, temp_df = train_test_split(
    df, test_size=0.3, random_state=42, stratify=df["label"],
)
val_df, test_df = train_test_split(
    temp_df, test_size=0.5, random_state=42, stratify=temp_df["label"],
)

print(f"Train: {len(train_df)} | Val: {len(val_df)} | Test: {len(test_df)}")
print(f"Label distribution (train): {train_df['label'].value_counts(normalize=True).to_dict()}")

# --- 4. Text Data Augmentation ---
import nlpaug.augmenter.word as naw

synonym_aug = naw.SynonymAug(aug_src="wordnet", aug_p=0.3)
backtranslation_aug = naw.BackTranslationAug(
    from_model_name="facebook/wmt19-en-de",
    to_model_name="facebook/wmt19-de-en",
)

def augment_dataset(df: pd.DataFrame, text_col: str, n_aug: int = 2) -> pd.DataFrame:
    """Augment text samples using synonym replacement and back-translation."""
    augmented_rows = []
    for _, row in df.iterrows():
        for _ in range(n_aug):
            aug_text = synonym_aug.augment(row[text_col])[0]
            augmented_rows.append({**row.to_dict(), text_col: aug_text, "augmented": True})
    aug_df = pd.DataFrame(augmented_rows)
    return pd.concat([df.assign(augmented=False), aug_df], ignore_index=True)

# --- 5. Bias Detection ---
def detect_bias(df: pd.DataFrame, label_col: str, sensitive_col: str) -> dict:
    """Check demographic parity and equalized odds across sensitive groups."""
    groups = df[sensitive_col].unique()
    metrics = {}

    # Demographic parity: P(Y=1) should be similar across groups
    for group in groups:
        group_df = df[df[sensitive_col] == group]
        positive_rate = (group_df[label_col] == 1).mean()
        metrics[f"positive_rate_{group}"] = round(positive_rate, 4)

    rates = [v for k, v in metrics.items() if k.startswith("positive_rate_")]
    metrics["demographic_parity_gap"] = round(max(rates) - min(rates), 4)
    metrics["bias_flag"] = metrics["demographic_parity_gap"] > 0.1

    return metrics

bias_report = detect_bias(train_df, label_col="label", sensitive_col="gender")
print(f"Bias report: {bias_report}")

# --- 6. Reproducibility Manifest ---
import json
from datetime import datetime

manifest = {
    "dataset_version": "v2.1",
    "created_at": datetime.utcnow().isoformat(),
    "random_seed": 42,
    "split_ratio": {"train": 0.7, "val": 0.15, "test": 0.15},
    "total_samples": len(df),
    "label_distribution": df["label"].value_counts().to_dict(),
    "augmentation": {"method": "synonym_replacement", "n_aug": 2},
    "bias_check": bias_report,
    "dependencies": {"pandas": pd.__version__, "sklearn": "1.4.0", "nlpaug": "1.1.11"},
}

with open("data/processed/manifest.json", "w") as f:
    json.dump(manifest, f, indent=2, default=str)
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `random_seed` | 42 | Seed for reproducible splits |
| `split_ratio` | 70/15/15 | Train/val/test proportions |
| `aug_p` | 0.3 | Probability of augmenting each word |
| `n_aug` | 2 | Number of augmented copies per sample |
| `bias_threshold` | 0.1 | Demographic parity gap flag threshold |
| DVC remote | S3/GCS | Remote storage for large datasets |

## Example Usage

```bash
# Full DVC workflow
dvc init
dvc remote add -d myremote s3://my-bucket/dvc-store
dvc add data/training_dataset.parquet
git add data/training_dataset.parquet.dvc .gitignore
git commit -m "Track training dataset v1"
dvc push

# Reproduce pipeline end-to-end
dvc repro

# Compare metrics across experiments
dvc metrics diff
```

## See Also

- [Feature Engineering](feature-engineering.md)
- [LLMOps Patterns](../concepts/llmops-patterns.md)
- [Embedding Pipelines](../concepts/embedding-pipelines.md)
