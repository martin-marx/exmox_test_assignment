# Databricks notebook source
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")

offers = (
    spark.table(f"{catalog}.bronze.offers")
    .withColumn("parsed_payout_eur", F.col("payout_eur").cast("decimal(10,2)"))
    .where(F.trim("offer_id").isNotNull() & (F.trim("offer_id") != ""))
    .where(F.col("parsed_payout_eur").isNotNull())
    .select(
        "offer_id",
        "offer_category",
        "payout_type",
        F.col("parsed_payout_eur").alias("payout_eur"),
    )
)

new_offers = offers.join(
    spark.table(f"{catalog}.silver.offers").select("offer_id"), "offer_id", "left_anti"
)
new_offers.write.format("delta").mode("append").saveAsTable(f"{catalog}.silver.offers")
