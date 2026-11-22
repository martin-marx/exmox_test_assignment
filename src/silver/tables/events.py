# Databricks notebook source
import os
import sys

# Notebook path is .../src/<layer>/<folder>/<name>; add .../src to sys.path so the utils import.
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path.insert(
    0, os.path.abspath(os.path.join("/Workspace" + os.path.dirname(_notebook_path), "..", ".."))
)
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.tables.utils import from_ingest_watermark

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")

silver_events = spark.table(f"{catalog}.silver.fact_events")
ingest_watermark = silver_events.agg(F.max("ingest_ts").alias("ingest_ts")).first().ingest_ts
new_events = (
    spark.table(f"{catalog}.bronze.events")
    .withColumn("_parsed_event_ts", F.try_to_timestamp("event_ts"))
    .withColumn("_parsed_ingest_ts", F.try_to_timestamp("ingest_ts"))
    .where(F.trim("event_id").isNotNull() & (F.trim("event_id") != ""))
    .where(F.trim("user_id").isNotNull() & (F.trim("user_id") != ""))
    .where(F.col("_parsed_event_ts").isNotNull() & F.col("_parsed_ingest_ts").isNotNull())
)
new_events = from_ingest_watermark(new_events, ingest_watermark)
deduplicated_events = (
    new_events.withColumn(
        "_row_number",
        F.row_number().over(
            Window.partitionBy("event_id").orderBy(F.col("_parsed_ingest_ts").asc())
        ),
    )
    .where("_row_number = 1")
    .select(
        "event_id",
        "user_id",
        F.col("_parsed_event_ts").alias("event_ts"),
        F.col("_parsed_ingest_ts").alias("ingest_ts"),
        F.to_date("_parsed_event_ts").alias("event_date"),
        "event_name",
        "offer_id",
    )
)
(
    DeltaTable.forName(spark, f"{catalog}.silver.fact_events")
    .alias("target")
    .merge(deduplicated_events.alias("source"), "target.event_id = source.event_id")
    .whenNotMatchedInsert(
        values={
            "event_id": "source.event_id",
            "user_id": "source.user_id",
            "event_ts": "source.event_ts",
            "ingest_ts": "source.ingest_ts",
            "event_date": "source.event_date",
            "event_name": "source.event_name",
            "offer_id": "source.offer_id",
        }
    )
    .execute()
)
