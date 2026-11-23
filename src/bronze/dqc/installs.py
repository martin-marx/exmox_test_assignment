# Databricks notebook source
"""Non-blocking source-data audits for installs."""

import logging

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")
logger = logging.getLogger(__name__)

installs = spark.table(f"{catalog}.bronze.installs")
invalid = installs.filter(
    F.col("user_id").isNull()
    | (F.trim("user_id") == "")
    | F.try_to_timestamp("install_ts").isNull()
).count()
if invalid:
    logger.warning("DATA_QUALITY_ALERT invalid_installs=%s", invalid)
duplicate_users = installs.count() - installs.select("user_id").distinct().count()
if duplicate_users:
    raise ValueError(
        f"Installs source must contain one row per user_id: {duplicate_users} duplicates"
    )

incoming_installs = (
    installs.withColumn("parsed_install_ts", F.try_to_timestamp("install_ts"))
    .where(F.trim("user_id").isNotNull() & (F.trim("user_id") != ""))
    .where(F.col("parsed_install_ts").isNotNull())
    .select(
        "user_id",
        F.col("parsed_install_ts").alias("install_ts"),
        F.upper(F.trim("country")).alias("country"),
        "platform",
        "media_source",
        "device_model",
        "campaign_id",
    )
)
install_definition_changes = (
    incoming_installs.alias("source")
    .join(spark.table(f"{catalog}.silver.installs").alias("target"), "user_id")
    .where(
        ~(
            F.col("source.install_ts").eqNullSafe(F.col("target.install_ts"))
            & F.col("source.country").eqNullSafe(F.col("target.country"))
            & F.col("source.platform").eqNullSafe(F.col("target.platform"))
            & F.col("source.media_source").eqNullSafe(F.col("target.media_source"))
            & F.col("source.device_model").eqNullSafe(F.col("target.device_model"))
            & F.col("source.campaign_id").eqNullSafe(F.col("target.campaign_id"))
        )
    )
    .count()
)
if install_definition_changes:
    raise ValueError(f"Immutable install definitions changed: {install_definition_changes}")
