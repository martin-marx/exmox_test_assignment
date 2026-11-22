"""Shared Spark transformations and Delta merge support for Silver modules."""

from pyspark.sql import functions as F


def normalize_country(column):
    return F.upper(F.trim(column))


def from_ingest_watermark(events, watermark):
    if watermark is None:
        return events
    return events.where(F.col("_parsed_ingest_ts") >= F.lit(watermark))


def merge_column_mappings(columns):
    return {column: f"source.{column}" for column in columns}


def merge_all(source, target, key, columns):
    from delta.tables import DeltaTable

    insert_mappings = merge_column_mappings(columns)
    update_mappings = merge_column_mappings([column for column in columns if column != key])
    (
        DeltaTable.forName(spark, target)
        .alias("target")
        .merge(source.alias("source"), f"target.{key} = source.{key}")
        .whenMatchedUpdate(set=update_mappings)
        .whenNotMatchedInsert(values=insert_mappings)
        .execute()
    )
