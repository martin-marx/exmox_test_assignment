# Databricks notebook source
"""Gold quality checks: unassigned rewards block; profile telemetry gaps are non-blocking audits."""

import logging

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")
logger = logging.getLogger(__name__)

unpriced_payouts = (
    spark.table(f"{catalog}.silver.fact_events")
    .where(F.col("event_name") == "reward_paid")
    .join(spark.table(f"{catalog}.silver.offers").select("offer_id"), "offer_id", "left_anti")
    .count()
)
if unpriced_payouts:
    raise ValueError(f"Reward payouts without an assigned offer reward: {unpriced_payouts}")

profile_gap = (
    spark.table(f"{catalog}.silver.user_profile")
    .filter(F.col("is_current") & (F.col("events_lifetime") > 0))
    .join(
        spark.table(f"{catalog}.silver.fact_events").select("user_id").distinct(),
        "user_id",
        "left_anti",
    )
    .count()
)
if profile_gap:
    logger.warning("DATA_QUALITY_ALERT profile_users_without_events=%s", profile_gap)
