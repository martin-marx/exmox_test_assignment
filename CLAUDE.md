# CLAUDE.md

This file provides strict technical guidance to Claude Code (`claude.ai/code`) when working with this repository.

## Commands & Environment

Dependencies and checks are managed through `uv`. The local `.venv` targets Python 3.12.

```bash
uv sync --all-groups
uv run ruff format --check .    # run without --check to auto-fix
uv run ruff check .
uv run pytest
```

Deployment and runs against the Databricks workspace:

```bash
./scripts/setup.sh
databricks bundle validate --target dev
databricks bundle deploy --target dev
databricks bundle run rewarded_offer_kpis --target dev --params target_date=2026-05-30,lookback_days=62
```

*Note: The synthetic source CSVs in `data/` are intentionally committed to the repository. Do not modify or delete them.*

## Code Architecture Constraints

**CRITICAL: Notebook vs. Module Separation**
* **Notebooks:** Every `.py` file under `src/bronze`, `src/silver`, and `src/gold` (except `utils.py` files) is a Databricks notebook. They execute top-down, rely on Databricks globals (`spark`, `dbutils`), and read parameters via `dbutils.widgets`. **Do not import notebooks as modules.**
* **Pure Functions:** Reusable transformation logic is isolated entirely in `utils.py` files. These are standard, importable Python modules.
* **Testing:** The `tests/` directory must only import from `utils.py` files. `ruff` is configured to ignore `F821` in notebook directories to accommodate globals.

## Pipeline & DQC Contracts

**Data Quality Checks (DQC):**
* **Non-Blocking (Audits):** Log `DATA_QUALITY_ALERT` via the `logging` module. Do not fail the pipeline for malformed source rows, unknown event names, or raw duplicates.
* **Blocking (Circuit Breakers):** Raise `ValueError` to halt downstream Silver tasks. Used exclusively for contract violations (e.g., duplicated immutable keys, altered historical payloads).

**Layer Paradigms:**
* **Bronze:** Overwrites raw snapshots as pure strings. Preserves all source fields exactly.
* **Silver:** Idempotent rules. Must handle append-only (installs/offers), incremental processing via `MAX(ingest_ts)` (events), and SCD Type 2 (profiles).
* **Gold:** Idempotent window replacement. Dynamically calculates `[target_date - lookback_days, target_date]` and applies an atomic `replaceWhere`.