"""Bronze ingestion for the four-entity workshop."""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

LANDING_PATH = spark.conf.get("landing_path")
SOURCE_FORMAT = spark.conf.get("source_format", "cloudFiles")
SCHEMA_TRACKING_PATH = spark.conf.get("schema_tracking_path", f"{LANDING_PATH}/_schemas")


def _schema_hints(schema: StructType) -> str:
    hints = []
    for field in schema.fields:
        if field.name == "_rescued_data":
            continue
        data_type = "BIGINT" if isinstance(field.dataType, LongType) else "STRING"
        hints.append(f"{field.name} {data_type}")
    return ", ".join(hints)


def _read(entity: str, schema: StructType) -> DataFrame:
    path = f"{LANDING_PATH}/{entity}"
    if SOURCE_FORMAT == "cloudFiles":
        frame = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", f"{SCHEMA_TRACKING_PATH}/{entity}")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .option("cloudFiles.schemaHints", _schema_hints(schema))
            .option("rescuedDataColumn", "_rescued_data")
            .load(path)
        )
    else:
        frame = spark.readStream.format(SOURCE_FORMAT).schema(schema).load(path)
    return (
        frame.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


merchant_schema = StructType(
    [
        StructField("merchant_id", StringType()),
        StructField("merchant_name", StringType()),
        StructField("country", StringType()),
        StructField("risk_tier", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)

order_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("order_ts", StringType()),
        StructField("amount_cents", LongType()),
        StructField("currency", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)

transaction_schema = StructType(
    [
        StructField("txn_id", StringType()),
        StructField("order_id", StringType()),
        StructField("merchant_id", StringType()),
        StructField("event_ts", StringType()),
        StructField("amount_cents", LongType()),
        StructField("currency", StringType()),
        StructField("processor", StringType()),
        StructField("status", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_incident_id", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)

dispute_schema = StructType(
    [
        StructField("dispute_id", StringType()),
        StructField("txn_id", StringType()),
        StructField("opened_at", StringType()),
        StructField("closed_at", StringType()),
        StructField("reason", StringType()),
        StructField("status", StringType()),
        StructField("_batch_id", StringType()),
        StructField("_incident_id", StringType()),
        StructField("_rescued_data", StringType()),
    ]
)


@dp.table(comment="Raw merchants with source and ingestion metadata")
def bronze_merchants() -> DataFrame:
    return _read("merchants", merchant_schema)


@dp.table(comment="Raw orders with source and ingestion metadata")
def bronze_orders() -> DataFrame:
    return _read("orders", order_schema)


@dp.table(comment="Raw transactions including deterministic incident identifiers")
def bronze_transactions() -> DataFrame:
    return _read("transactions", transaction_schema)


@dp.table(comment="Raw disputes; the held late chargeback arrives only during replay")
def bronze_disputes() -> DataFrame:
    return _read("disputes", dispute_schema)
