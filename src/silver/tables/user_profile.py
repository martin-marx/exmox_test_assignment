# Databricks notebook source

from delta.tables import DeltaTable
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")

incoming_profiles = (
    spark.table(f"{catalog}.bronze.user_profile")
    .where(F.trim("user_id").isNotNull() & (F.trim("user_id") != ""))
    .select(
        "user_id",
        F.col("events_lifetime").cast("bigint").alias("events_lifetime"),
        F.try_to_timestamp("last_seen_ts").alias("last_seen_ts"),
        F.col("revenue_30d_eur").cast("decimal(10,2)").alias("revenue_30d_eur"),
        (F.lower("is_payer") == F.lit("true")).alias("is_payer"),
    )
)
current_profiles = spark.table(f"{catalog}.silver.user_profile").where("is_current")
profile_changes = (
    incoming_profiles.alias("source")
    .join(
        current_profiles.alias("target"),
        F.col("source.user_id") == F.col("target.user_id"),
        "left",
    )
    .where(
        F.col("target.user_id").isNull()
        | ~(
            F.col("source.events_lifetime").eqNullSafe(F.col("target.events_lifetime"))
            & F.col("source.last_seen_ts").eqNullSafe(F.col("target.last_seen_ts"))
            & F.col("source.revenue_30d_eur").eqNullSafe(F.col("target.revenue_30d_eur"))
            & F.col("source.is_payer").eqNullSafe(F.col("target.is_payer"))
        )
    )
    .select("source.*", F.col("target.user_id").isNotNull().alias("has_current_version"))
    .withColumn("effective_at", F.current_timestamp())
)

expiry_rows = profile_changes.where("has_current_version").select(
    F.col("user_id").alias("merge_user_id"),
    F.lit("expire").alias("action"),
    F.lit(None).cast("string").alias("user_id"),
    F.lit(None).cast("bigint").alias("events_lifetime"),
    F.lit(None).cast("timestamp").alias("last_seen_ts"),
    F.lit(None).cast("decimal(10,2)").alias("revenue_30d_eur"),
    F.lit(None).cast("boolean").alias("is_payer"),
    "effective_at",
)
insert_rows = profile_changes.select(
    F.lit(None).cast("string").alias("merge_user_id"),
    F.lit("insert").alias("action"),
    "user_id",
    "events_lifetime",
    "last_seen_ts",
    "revenue_30d_eur",
    "is_payer",
    "effective_at",
)
staged_changes = expiry_rows.unionByName(insert_rows)

(
    DeltaTable.forName(spark, f"{catalog}.silver.user_profile")
    .alias("target")
    .merge(
        staged_changes.alias("source"),
        "target.user_id = source.merge_user_id AND target.is_current = true",
    )
    .whenMatchedUpdate(
        condition="source.action = 'expire'",
        set={
            "valid_to": "source.effective_at",
            "is_current": "false",
        },
    )
    .whenNotMatchedInsert(
        condition="source.action = 'insert'",
        values={
            "user_id": "source.user_id",
            "events_lifetime": "source.events_lifetime",
            "last_seen_ts": "source.last_seen_ts",
            "revenue_30d_eur": "source.revenue_30d_eur",
            "is_payer": "source.is_payer",
            "valid_from": "source.effective_at",
            "valid_to": "CAST(NULL AS TIMESTAMP)",
            "is_current": "true",
        },
    )
    .execute()
)
