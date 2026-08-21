"""Generate deterministic workshop data into a local path or Unity Catalog Volume."""

from __future__ import annotations

import argparse
import json
from hashlib import sha256
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from gen.synthetic.generator import GeneratorConfig, build_frames, manifest
except ModuleNotFoundError:  # Direct Databricks spark_python_task execution.
    from generator import GeneratorConfig, build_frames, manifest


def _safe_output(value: str) -> str:
    normalized = value.rstrip("/")
    if normalized in {"", ".", "/", "/Volumes"}:
        raise ValueError("output must be a specific local directory or Unity Catalog Volume child path")
    return normalized


def _semantic_digest(frame: DataFrame) -> str:
    """Hash sorted canonical row hashes without collecting the dataset into driver memory."""
    columns = sorted(frame.columns)
    row_hashes = frame.select(
        F.sha2(F.to_json(F.struct(*[F.col(column) for column in columns])), 256).alias("row_hash")
    ).orderBy("row_hash")
    digest = sha256()
    for row in row_hashes.toLocalIterator():
        digest.update(row.row_hash.encode("ascii"))
        digest.update(b"\n")
    return f"sha256:{digest.hexdigest()}"


def generate(output: str, seed: int, partitions: int) -> dict[str, object]:
    target = _safe_output(output)
    spark = SparkSession.builder.appName("dbx-agentic-synthetic-generator").getOrCreate()
    frames = build_frames(spark, GeneratorConfig(seed=seed, partitions=partitions))
    entity_digests = {name: _semantic_digest(frame) for name, frame in frames.items()}

    for name, frame in frames.items():
        if name == "disputes":
            active = frame.filter(F.col("_release_state") == "active").drop("_release_state")
            held = frame.filter(F.col("_release_state") == "held").drop("_release_state")
            active.write.mode("overwrite").option("compression", "gzip").json(f"{target}/disputes")
            held.write.mode("overwrite").option("compression", "gzip").json(f"{target}/held/disputes")
        else:
            frame.write.mode("overwrite").option("compression", "gzip").json(f"{target}/{name}")

    result = manifest(GeneratorConfig(seed=seed, partitions=partitions))
    result["entity_digests"] = entity_digests
    result.pop("logical_digest")
    canonical = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
    result["logical_digest"] = f"sha256:{sha256(canonical).hexdigest()}"
    spark.createDataFrame([(json.dumps(result, sort_keys=True),)], "value string").coalesce(1).write.mode(
        "overwrite"
    ).text(f"{target}/_manifest")
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    parser.add_argument("--seed", type=int, default=22_082_026)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--manifest-out")
    args = parser.parse_args()
    result = generate(args.output, args.seed, args.partitions)
    if args.manifest_out:
        destination = Path(args.manifest_out)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
