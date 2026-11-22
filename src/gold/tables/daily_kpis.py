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
from datetime import date, timedelta

from gold.tables.utils import aggregate_events, aggregate_installs, combine_daily_kpis

dbutils.widgets.text("catalog", "test_assignment_marx")
dbutils.widgets.text("target_date", "")
dbutils.widgets.text("lookback_days", "7")
catalog = dbutils.widgets.get("catalog")
target_date_parameter = dbutils.widgets.get("target_date")
lookback_days = int(dbutils.widgets.get("lookback_days"))

if not target_date_parameter:
    raise ValueError("target_date is required and must use YYYY-MM-DD format")
if lookback_days < 0:
    raise ValueError("lookback_days must be non-negative")

end_date = date.fromisoformat(target_date_parameter)
start_date = end_date - timedelta(days=lookback_days)
gold_table = f"{catalog}.gold.daily_kpis"

installs = spark.table(f"{catalog}.silver.installs")
events = spark.table(f"{catalog}.silver.fact_events")
offers = spark.table(f"{catalog}.silver.offers")
installs_by_day = aggregate_installs(installs, start_date, end_date)
events_by_day = aggregate_events(events, installs, offers, start_date, end_date)
daily_kpis = combine_daily_kpis(installs_by_day, events_by_day)

replace_where = f"report_date >= DATE '{start_date.isoformat()}' AND report_date <= DATE '{end_date.isoformat()}'"
daily_kpis.write.format("delta").mode("overwrite").option(
    "replaceWhere", replace_where
).saveAsTable(gold_table)
