# Databricks notebook source
"""Non-blocking source-data audits for events."""

import logging

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
dbutils.widgets.text("lookback_days", "7")
catalog = dbutils.widgets.get("catalog")
lookback_days = int(dbutils.widgets.get("lookback_days"))
logger = logging.getLogger(__name__)

events = spark.table(f"{catalog}.bronze.events")
invalid_rows = events.filter(
    F.col("event_id").isNull()
    | (F.trim("event_id") == "")
    | F.col("user_id").isNull()
    | (F.trim("user_id") == "")
    | F.try_to_timestamp("event_ts").isNull()
    | F.try_to_timestamp("ingest_ts").isNull()
).count()
unknown_names = events.filter(
    F.col("event_name").isNull()
    | ~F.col("event_name").isin(
        "app_open", "offer_view", "offer_start", "goal_reached", "reward_paid"
    )
).count()
duplicates = events.count() - events.select("event_id").distinct().count()
late_beyond_lookback = (
    events.withColumn("event_timestamp", F.try_to_timestamp("event_ts"))
    .withColumn("ingest_timestamp", F.try_to_timestamp("ingest_ts"))
    .filter(F.col("ingest_timestamp") > F.expr(f"event_timestamp + INTERVAL {lookback_days} DAYS"))
    .count()
)
if invalid_rows or unknown_names or duplicates or late_beyond_lookback:
    logger.warning(
        "DATA_QUALITY_ALERT invalid_events=%s unknown_event_names=%s duplicate_raw_rows=%s late_beyond_lookback=%s",
        invalid_rows,
        unknown_names,
        duplicates,
        late_beyond_lookback,
    )

valid_events = (
    events.withColumn("parsed_event_ts", F.try_to_timestamp("event_ts"))
    .withColumn("parsed_ingest_ts", F.try_to_timestamp("ingest_ts"))
    .where(F.trim("event_id").isNotNull() & (F.trim("event_id") != ""))
    .where(F.trim("user_id").isNotNull() & (F.trim("user_id") != ""))
    .where(F.col("parsed_event_ts").isNotNull() & F.col("parsed_ingest_ts").isNotNull())
)
event_payload_conflicts = (
    valid_events.groupBy("event_id")
    .agg(
        F.countDistinct(F.struct("user_id", "parsed_event_ts", "event_name", "offer_id")).alias(
            "payload_count"
        )
    )
    .where(F.col("payload_count") > 1)
    .count()
)
if event_payload_conflicts:
    raise ValueError(f"Event IDs have conflicting business payloads: {event_payload_conflicts}")

event_payload_conflicts_with_silver = (
    valid_events.alias("source")
    .join(spark.table(f"{catalog}.silver.fact_events").alias("target"), "event_id")
    .where(
        ~(
            F.col("source.user_id").eqNullSafe(F.col("target.user_id"))
            & F.col("source.parsed_event_ts").eqNullSafe(F.col("target.event_ts"))
            & F.col("source.event_name").eqNullSafe(F.col("target.event_name"))
            & F.col("source.offer_id").eqNullSafe(F.col("target.offer_id"))
        )
    )
    .count()
)
if event_payload_conflicts_with_silver:
    raise ValueError(
        f"Existing event IDs have changed business payloads: {event_payload_conflicts_with_silver}"
    )
