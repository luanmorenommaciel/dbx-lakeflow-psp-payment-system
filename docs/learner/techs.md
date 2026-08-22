# Techs

Each tool has one job on this incident. Do not add a second platform.

| Tech | Job on this ticket |
|---|---|
| **dbldatagen** | Build the seeded 100,000-transaction story (`seed=22082026`) |
| **Lakeflow Spark Declarative Pipelines** | One graph: Bronze → DQX split → Gold. Use `from pyspark import pipelines as dp` |
| **DQX** | Sole domain-quality source. Valid rows to Silver; invalid rows to quarantine with `_errors` / `_warnings` |
| **Databricks Asset Bundles** | Deploy only the `dev` target to your Free Edition workspace |
| **Unity Catalog** | One schema, one managed Volume, lineage, comments |
| **Genie Code** | Investigate Gold and quarantine in natural language; inspect the SQL |
| **Databricks Agent Skills** | Teach the agent current Databricks APIs. They do not replace the BRD or contracts |

Authorized currencies are `USD`, `GBP`, `CAD`, and `AUD`. Processors are `stripe` and `adyen`. `BRL` is
unauthorized on purpose. Do not add customers, payment instruments, payouts, Azure Blob, or a production target.
