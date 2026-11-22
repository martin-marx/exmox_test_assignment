# Databricks notebook source
"""Non-blocking audits for published KPI data and expected upstream gaps."""

import logging

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")
logger = logging.getLogger(__name__)

unpriced = (
    spark.table(f"{catalog}.gold.daily_kpis")
    .agg(F.coalesce(F.sum("unpriced_reward_payouts"), F.lit(0)).alias("unpriced"))
    .first()
    .unpriced
)
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
if unpriced or profile_gap:
    logger.warning(
        "DATA_QUALITY_ALERT unpriced_reward_payouts=%s profile_users_without_events=%s",
        unpriced,
        profile_gap,
    )
