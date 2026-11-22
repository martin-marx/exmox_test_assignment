# Rewarded-Offer KPIs

Databricks batch pipeline for daily rewarded-offer KPIs by reporting date, country, and platform.

## Prerequisites

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)
- Databricks CLI authenticated to a Unity Catalog-enabled workspace
- A SQL warehouse ID for the one-time schema setup
- The supplied `data/` directory with the four source CSVs; they are intentionally not committed to Git

## Local Checks

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run pytest
```

## First Workspace Run

Authenticate the Databricks CLI, then set the SQL warehouse used for the DDL:

```bash
databricks auth login --host https://<workspace-host>
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
```

Create the Unity Catalog objects and upload the supplied static source files:

```bash
./scripts/setup.sh
```

Deploy and run the Workflow:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run rewarded_offer_kpis --target dev \
  --params target_date=2026-05-30,lookback_days=62
```

The initial supplied-data load uses `2026-05-30` and `62` days to cover the complete date range. Compare the resulting Gold table with the expected values in `SOLUTION.md`.

Scheduled runs resolve `target_date` to the previous UTC calendar date using Databricks Jobs date arithmetic. This requires the workspace preview **Math in Parameters – Expression support in job parameters**. Every run rebuilds the inclusive window ending on `target_date`. To rebuild one reporting date manually:

```bash
databricks bundle run rewarded_offer_kpis --target dev \
  --params target_date=2026-05-15,lookback_days=0
```

## Pipeline Layout

```text
src/
  bronze/
    sources/  # One parameterized raw-snapshot ingestion notebook
    dqc/      # Source-data audits
  silver/
    tables/   # Typed per-table transformations
  gold/
    tables/   # Daily KPI aggregation
    dqc/      # Published KPI audits
tests/
  bronze/
  silver/
  gold/
```

Bronze overwrites each static raw snapshot as strings. Re-running the same input leaves Bronze unchanged; Silver event inserts, immutable install/offer inserts, profile SCD2, and Gold window replacement keep the pipeline idempotent. The events DQC validates the full current Bronze snapshot against Silver history before Silver filters its new `ingest_ts` batch. Offers and installs must contain one immutable definition per business key, so duplicate IDs or changed existing definitions block their Silver branches. User profiles must have one row per user in the source; changed profiles atomically expire the current Silver version and insert a replacement. Silver cleans and types data; unusable keyed or dated rows remain available in Bronze and produce non-blocking DQC alerts. A Delta `CHECK` constraint blocks negative payouts when `silver.offers` is written. Gold depends only on Silver installs, events, and offers.

Silver events uses `MAX(silver.fact_events.ingest_ts)` as its source-arrival watermark. It reprocesses the watermark boundary, deduplicates it by `event_id`, and inserts only unseen event IDs. Gold remains strictly controlled by its `target_date` and `lookback_days` window. This assumes `ingest_ts` is a reliable source high watermark.

Each Silver branch waits for its Bronze DQC. Unknown event names are retained for audit but do not contribute to a named Gold event metric.

See `SOLUTION.md` for data findings, layer decisions, and reference output values.
