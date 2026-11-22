# Databricks notebook source
"""Enforce the one-row-per-user profile snapshot grain."""

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "test_assignment_marx")
catalog = dbutils.widgets.get("catalog")
profiles = spark.table(f"{catalog}.bronze.user_profile")
invalid = profiles.filter(F.col("user_id").isNull() | (F.trim("user_id") == "")).count()
duplicate_users = profiles.count() - profiles.select("user_id").distinct().count()
if invalid or duplicate_users:
    raise ValueError(
        f"User profile source must contain one valid row per user_id: invalid={invalid}, duplicates={duplicate_users}"
    )
