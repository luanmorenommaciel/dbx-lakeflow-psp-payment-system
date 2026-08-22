# Module 03 — Bronze intake

Read `AGENTS.md`, the product specification, the contracts, and the Lakeflow/streaming skills. First propose a
plan and wait for my approval.

After approval, create `pipelines/src/psp-agentic/bronze.py` and the cumulative local
`pipelines/spark-pipeline.yaml`. Build four streaming Bronze tables with the current `pyspark.pipelines` API.
Hosted input must use Auto Loader with explicit schema tracking, `addNewColumns` evolution, and
`_rescued_data`; every row must retain source file, batch, and ingestion lineage. The local JSON path must build
the same graph. Do not add Silver, Gold, or remote resources. Run `./scripts/checkpoint.sh 03`, explain what the
dry-run proves and does not prove, and stop.
