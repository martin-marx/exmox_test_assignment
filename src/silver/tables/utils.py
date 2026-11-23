"""Shared Spark transformations for Silver modules."""

from pyspark.sql import functions as F


def normalize_country(column):
    return F.upper(F.trim(column))


def from_ingest_watermark(events, watermark):
    if watermark is None:
        return events
    return events.where(F.col("_parsed_ingest_ts") >= F.lit(watermark))
