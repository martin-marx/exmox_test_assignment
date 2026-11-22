# Databricks notebook source
"""Audit offer quality and enforce the one-row-per-offer source grain."""

import logging

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")
logger = logging.getLogger(__name__)

offers = spark.table(f"{catalog}.bronze.offers").withColumn(
    "parsed_payout_eur", F.col("payout_eur").cast("decimal(10,2)")
)
malformed = offers.filter(
    F.col("offer_id").isNull() | (F.trim("offer_id") == "") | F.col("parsed_payout_eur").isNull()
).count()
negative = offers.filter(F.col("parsed_payout_eur") < 0).count()
if malformed or negative:
    logger.warning(
        "DATA_QUALITY_ALERT malformed_offers=%s negative_payouts=%s", malformed, negative
    )

duplicate_offer_rows = offers.count() - offers.select("offer_id").distinct().count()
if duplicate_offer_rows:
    raise ValueError(
        f"Offers source must contain one row per offer_id: {duplicate_offer_rows} duplicates"
    )

incoming_offers = offers.where(
    F.trim("offer_id").isNotNull()
    & (F.trim("offer_id") != "")
    & F.col("parsed_payout_eur").isNotNull()
).select(
    "offer_id",
    "offer_category",
    "payout_type",
    F.col("parsed_payout_eur").alias("payout_eur"),
)
offer_definition_changes = (
    incoming_offers.alias("source")
    .join(spark.table(f"{catalog}.silver.offers").alias("target"), "offer_id")
    .where(
        ~(
            F.col("source.offer_category").eqNullSafe(F.col("target.offer_category"))
            & F.col("source.payout_type").eqNullSafe(F.col("target.payout_type"))
            & F.col("source.payout_eur").eqNullSafe(F.col("target.payout_eur"))
        )
    )
    .count()
)
if offer_definition_changes:
    raise ValueError(f"Immutable offer definitions changed: {offer_definition_changes}")
