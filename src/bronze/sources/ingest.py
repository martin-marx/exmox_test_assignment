# Databricks notebook source
import os
import sys

# Notebook path is .../src/<layer>/<folder>/<name>; add .../src to sys.path so the utils import.
_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
sys.path.insert(
    0, os.path.abspath(os.path.join("/Workspace" + os.path.dirname(_notebook_path), "..", ".."))
)
from bronze.sources.utils import ingest_source, parse_source_columns, source_path

dbutils.widgets.text("catalog", "test_assignment_marx")
dbutils.widgets.text("source_name", "")
dbutils.widgets.text("target_table", "")
dbutils.widgets.text("source_columns", "[]")

catalog = dbutils.widgets.get("catalog")
source_name = dbutils.widgets.get("source_name")
target_table = dbutils.widgets.get("target_table")
source_columns = parse_source_columns(dbutils.widgets.get("source_columns"))

if not source_name or not target_table:
    raise ValueError("source_name and target_table are required")

incoming = ingest_source(spark, source_path(catalog, source_name), source_columns)
incoming.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{target_table}")
