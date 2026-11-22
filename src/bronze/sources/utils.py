"""Shared raw CSV ingestion helpers for append-only Bronze tables."""

import json

from pyspark.sql import functions as F


def source_path(catalog, entity):
    return f"/Volumes/{catalog}/data/raw_data/{entity}.csv"


def parse_source_columns(value):
    columns = json.loads(value)
    if not isinstance(columns, list) or not all(isinstance(column, str) for column in columns):
        raise ValueError("source_columns must be a JSON array of strings")
    return columns


def ingest_source(spark, input_path, columns):
    """Read one CSV source as raw Bronze columns and metadata."""
    return (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .csv(input_path)
        .select(*[F.col(name).cast("string") for name in columns])
    )
