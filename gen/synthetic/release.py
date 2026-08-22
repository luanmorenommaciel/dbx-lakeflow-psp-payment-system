"""Release the held late chargeback into the active dispute landing path."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession

try:
    from gen.synthetic.cli import _safe_output
except ModuleNotFoundError:  # Direct Databricks spark_python_task execution.
    from cli import _safe_output


def release(output: str) -> bool:
    target = _safe_output(output)
    spark = SparkSession.builder.appName("dbx-agentic-incident-release").getOrCreate()
    active = spark.read.json(f"{target}/disputes")
    if active.where("_incident_id = 'late-valid-chargeback'").limit(1).count():
        return False
    held = spark.read.json(f"{target}/held/disputes")
    held.write.mode("append").option("compression", "gzip").json(f"{target}/disputes")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    released = release(args.output)
    print("INCIDENT_REPLAY=RELEASED" if released else "INCIDENT_REPLAY=ALREADY_RELEASED")


if __name__ == "__main__":
    main()
