# Databricks notebook source

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")

incoming_installs = (
    spark.table(f"{catalog}.bronze.installs")
    .withColumn("parsed_install_ts", F.try_to_timestamp("install_ts"))
    .where(F.trim("user_id").isNotNull() & (F.trim("user_id") != ""))
    .where(F.col("parsed_install_ts").isNotNull())
    .select(
        "user_id",
        F.col("parsed_install_ts").alias("install_ts"),
        F.to_date("parsed_install_ts").alias("install_date"),
        F.upper(F.trim("country")).alias("country"),
        "platform",
        "media_source",
        "device_model",
        "campaign_id",
    )
)
new_installs = incoming_installs.join(
    spark.table(f"{catalog}.silver.installs").select("user_id"), "user_id", "left_anti"
)
new_installs.write.format("delta").mode("append").saveAsTable(f"{catalog}.silver.installs")
