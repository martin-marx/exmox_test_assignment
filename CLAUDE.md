# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependencies and checks run through `uv` (Python 3.11–3.13; the local `.venv` is 3.12):

```bash
uv sync --all-groups
uv run ruff format --check .    # `uv run ruff format .` to fix
uv run ruff check .
uv run pytest
uv run pytest tests/gold/test_gold_utils.py::test_aggregate_events_calculates_metrics_cost_and_unpriced_payouts
```

The Spark tests need a Java runtime on the machine (PySpark starts a JVM). On this machine `java` is not installed, so 11 of 14 tests error at fixture setup with `PySparkRuntimeError`; only the three non-Spark tests pass. Install a JDK before expecting the suite to be green.

Deployment and runs against the Databricks workspace (requires `databricks auth login`, and `DATABRICKS_WAREHOUSE_ID` for setup):

```bash
./scripts/setup.sh                      # applies sql/ddl.sql, uploads CSVs to the Volume (see note below)
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run rewarded_offer_kpis --target dev --params target_date=2026-05-30,lookback_days=62
```

The supplied inputs live in `data/` (`data/TASK.md` is the assignment spec; `data/*.csv` are the four source files). That folder is untracked and must stay out of Git. `scripts/setup.sh` uploads from `data/${entity}.csv`. `SOLUTION.md` lists the reference values the full load should reproduce (997 Gold rows, 40,000 installs, 9,860 payouts, EUR 26,547.41).

## Architecture

This is a Databricks Asset Bundle with one Workflow (`resources/workflow.yml`) implementing a Bronze → Silver → Gold medallion pipeline over four static CSVs (installs, events, offers, user_profile).

**Every `.py` under `src/bronze|silver|gold/{sources,tables,dqc}` except `utils.py` is a Databricks notebook, not an importable module.** Top-level code runs on import and uses the `spark` and `dbutils` globals. Each reads its inputs through `dbutils.widgets` (`catalog`, plus task-specific parameters), which the Workflow passes in. Only `src/{bronze/sources,silver/tables,gold/tables}/utils.py` contain importable pure functions. Notebooks add `src/` to `sys.path` (from `dbutils` notebook context) and import them, and tests import them directly as `gold.tables.utils`, `silver.tables.utils`, and `bronze.sources.utils` (`pythonpath = ["src"]` in `pyproject.toml`). That is why ruff ignores `F821` for notebook files.

**Task graph.** `resources/workflow.yml` runs per source, with a chain for each: `bronze_<src>` → `dqc_bronze_<src>` → `silver_<src>`. Gold waits on `silver_installs`, `silver_events`, and `silver_offers`. `dqc_gold_daily_kpis` also waits on `silver_user_profile`, so the profile branch is not on the Gold data path. The Workflow owns the source column contracts (`source_columns` in each Bronze task). `sql/ddl.sql` owns the table schemas. Changing a column means updating both.

**Bronze** (`bronze/sources/ingest.py`): overwrites each `bronze.<entity>` table from `/Volumes/<catalog>/data/raw_data/<entity>.csv`, casting every column to string. Bronze keeps every raw row for audit.

**Bronze DQC** (`bronze/dqc/*.py`) has two kinds of check, and the difference matters:
- Non-blocking: logs `DATA_QUALITY_ALERT` for malformed keys, bad timestamps, unknown event names, and raw duplicates.
- Blocking: raises `ValueError` on duplicate offer IDs, changed payloads for an existing event ID, or changed definitions for an existing offer ID. A blocking failure stops that source's Silver task.

**Silver** (`silver/tables/*.py`) is where business rules live. Each notebook writes its own table, with no shared merge helper:
- `installs` and `offers` are immutable. Only keys not yet in Silver are appended (`left_anti` join).
- `fact_events` is incremental. It starts from `MAX(ingest_ts)` in Silver, which reprocesses that boundary, then keeps the earliest `ingest_ts` per `event_id` and merges on `event_id` plus `event_date` (insert-only, so unseen events are inserted).
- `user_profile` is SCD2. A single Delta `MERGE` both expires the current version of changed users and inserts their new version. Rows that fail key or timestamp parsing are dropped from Silver but remain in Bronze.
- A Delta `CHECK (payout_eur >= 0)` on `silver.offers` is the last line of defence against negative payouts.

**Gold** (`gold/tables/daily_kpis.py`) rebuilds the window `[target_date − lookback_days, target_date]` in `gold.daily_kpis` with `replaceWhere`. It computes installs and event metrics separately, then full-outer-joins them on `(report_date, country, platform)` so install-only combinations survive. Event country and platform come from the user's install (a join on `user_id`), not from the event itself. Events with an offer missing from `silver.offers` are kept with a left join, so they count toward events but not toward cost. Distinct-user counts are non-additive, so the window is recomputed rather than summed. `gold/dqc/daily_kpis.py` is also non-blocking.

**Re-runs.** Each Silver step is designed to be idempotent: Bronze overwrites, Silver is append-only or merges on keys, and Gold rewrites a bounded window. If you change an incremental rule, check the reprocessing behaviour, not just the happy path.
