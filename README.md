# Rewarded-Offer KPIs

Databricks batch pipeline for daily rewarded-offer KPIs by reporting date, country, and platform.

## Prerequisites

* Python 3.11–3.13 (managed via [uv](https://docs.astral.sh/uv/))
* Databricks CLI (tested with v1.27) authenticated to a Unity Catalog-enabled workspace
* `jq` (used by `scripts/setup.sh`)
* Permission to create catalogs in the workspace (or a pre-created catalog named `test_assignment_marx`)
* A running SQL warehouse ID for the one-time schema setup
* **JDK 17** (required for local PySpark unit tests only)
* The four source CSVs (`installs`, `events`, `offers`, `user_profile`) in `data/` (already committed to the repository).

## Local Checks

```bash
uv sync --all-groups
uv run ruff format --check .
uv run ruff check .
uv run pytest
```

## First Workspace Run

Authenticate the Databricks CLI, then find and export the SQL warehouse used for the DDL. Run these from the repository root:

```bash
databricks auth login --host https://<workspace-host>
databricks warehouses list
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
```

Create the Unity Catalog objects and upload the supplied static source files:

```bash
./scripts/setup.sh
```
*Note: This script creates the catalog `test_assignment_marx`, the schemas (`bronze`, `silver`, `gold`, `data`), the volume `data.raw_data`, and all tables, then uploads the CSVs to `/Volumes/test_assignment_marx/data/raw_data/`. It is re-runnable.*

Deploy and run the Workflow:

```bash
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run rewarded_offer_kpis --target dev \
  --params target_date=2026-05-30,lookback_days=62
```

The initial data load uses `2026-05-30` and `62` days to cover the complete supplied date range.

Scheduled runs resolve `target_date` to the previous UTC calendar date automatically. To restate a single historical reporting date manually:

```bash
databricks bundle run rewarded_offer_kpis --target dev \
  --params target_date=2026-05-15,lookback_days=0
```

## Repository Structure

```text
.
├── scripts/
│   └── setup.sh              # Bootstraps Unity Catalog and uploads CSVs
├── sql/
│   ├── ddl.sql               # Declarative schema and table definitions
│   └── grants.sql            # Reference RBAC policies
├── src/
│   ├── bronze/               # Raw string ingestion and Data Quality Checks (DQC)
│   ├── silver/               # Cleansing, deduplication, and SCD models
│   └── gold/                 # Parameterized window aggregations
├── tests/                    # Pytest suite for pure transformation logic
├── SOLUTION.md               # Architecture decisions, data findings, and KPI results
└── CLAUDE.md                 # Technical guide and constraints for Claude Code
```

Please refer to **`SOLUTION.md`** for a complete breakdown of data findings, architectural trade-offs, and the reference output values.