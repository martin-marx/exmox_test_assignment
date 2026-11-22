# Rewarded-Offer Daily KPIs

## Environment and Runbook

The code targets Databricks Free Edition serverless compute with the `test_assignment_marx` catalog. It has not been run in a personal Databricks workspace; the reference results below were independently recomputed from the supplied CSVs. A production workspace would use separate environment catalogs and service-principal grants.

1. Authenticate the Databricks CLI and set `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, and `DATABRICKS_WAREHOUSE_ID`.
2. Ensure the supplied `data/` CSVs are present, then run `scripts/setup.sh`. It applies `sql/ddl.sql` and uploads each static source file to the Unity Catalog Volume.
3. Run `databricks bundle deploy`, then trigger `rewarded-offer-kpis` with `target_date=2026-05-30,lookback_days=62` for the supplied initial load. The bundled schedule is paused by default and is `00:15 UTC` once enabled; scheduled target dates resolve to the previous UTC calendar date through Databricks Jobs date arithmetic. This requires the workspace preview **Math in Parameters – Expression support in job parameters**.
4. Run `uv sync --all-groups`, `uv run ruff format --check .`, `uv run ruff check .`, and `uv run pytest` locally.

The Workflow fans out by source: each Bronze table runs independently, then its table-specific DQC starts immediately. Each Silver transformation waits for its DQC. A Delta `CHECK` constraint blocks a negative payout when Silver offers are written. Gold waits only for Silver installs, events, and offers; its DQC also waits for the profile SCD2 branch to complete before auditing profile gaps. Gold always rebuilds only the explicit `target_date` and `lookback_days` window. `lookback_days=0` restates one day.

## Layers and Re-runs

Bronze stores the current raw snapshot: one parameterized notebook receives a source name, target table, and source-column contract from the Workflow, then overwrites that raw table. All CSV values remain strings. Re-running the same static inputs leaves Bronze unchanged; full-snapshot DQCs, watermark-filtered Silver event inserts, immutable install/offer inserts, profile SCD2, and Gold window replacement keep the pipeline idempotent. A production feed would use append-only immutable files with file tracking or a source version/checksum.

Silver owns business interpretation: country normalization, typed fields, earliest-`ingest_ts` event deduplication, immutable installs/offers, and user-profile SCD2. Events are incrementally selected from Bronze at or beyond `MAX(silver.fact_events.ingest_ts)` and reprocess the boundary safely. This assumes `ingest_ts` is a reliable source high watermark. Gold remains controlled only by `target_date` and `lookback_days`, and joins offers on `offer_id`; user profiles remain audit-only. Rows without usable keys or timestamps stay available in Bronze but are excluded from typed Silver metrics; the DQC reports their counts. Installs and offers must have one immutable definition per business key: duplicate or changed existing definitions block their Silver branches. Profiles version only when their tracked values change.

Gold has `(report_date, country, platform)` grain. It full-outer-joins daily installs with event metrics so install-only combinations remain visible. The bounded overwrite is idempotent. Distinct-user counts are non-additive, so the restatement window is recalculated rather than incrementally summed.

## Data Findings

- 435,907 raw events reduce to 423,186 unique `event_id`s, so Silver keeps the earliest `ingest_ts` version.
- 13,270 unique non-payment events use offers missing from the offer file. Gold preserves them with a left join. All 9,860 reward payments are priced; `unpriced_reward_payouts` is therefore zero for this data, but remains a soft audit metric for future data.
- Event ingestion lag peaks at 133.9 hours, with 2.51% arriving after 24 hours. The seven-day default exceeds the observed tail but must be monitored and adjusted from lag percentiles.
- 1,590 country values need trimming and uppercasing. Organic campaign IDs are valid NULLs and remain NULL.
- 3,003 events predate their installation timestamp. They are retained because client clock skew and non-linear funnels are plausible mobile telemetry behavior.
- 2,305 profiles report lifetime events but have no event telemetry. Profiles are retained for audit only and excluded from KPI calculations.

Expected full-load result: 997 Gold rows, 40,000 installs, 9,860 payouts, EUR 26,547.41 reward cost, and zero unpriced payouts. The `2026-05-15 / DE / android` slice is 77 installs, 323 app opens, 194 views, 137 starts, 55 goals, 15 paying users, 15 payouts, and EUR 33.32 cost. A live workspace execution remains required before submission.

## Quality Controls

Runtime DQCs validate incoming data rather than re-testing transformation logic. Blank required identifiers, unparseable timestamps, unknown event names, raw duplicates, malformed offer values, unpriced payouts, and profile telemetry gaps are non-blocking audits. Bronze preserves every source row for investigation. Duplicate or changed immutable offer/install definitions block their Silver branches, and duplicate profile IDs block profile processing. Malformed payouts are excluded from Silver and surface as unpriced payouts; a Delta `CHECK (payout_eur >= 0)` blocks negative numeric payouts. PySpark unit tests cover ingestion, normalization, deduplication, missing-offer preservation, and full-outer aggregation.

## Deeper Considerations

Filtering a nightly job only by yesterday's `event_ts` loses late events; assigning late arrivals to their ingest date corrupts daily trends. The implementation restates a bounded trailing window, and its events DQC logs rows arriving beyond that window. This is a bounded correction policy, not a guarantee to repair arbitrarily old arrivals; production would route affected event dates from incremental ingestion state.

Liquid clustering avoids tiny date partitions for this small data volume. At scale, validate it with query-profile pruning metrics and change keys to follow access patterns. Unity Catalog should centrally govern catalogs, ownership, grants, and shared dimensions; that adds change control but prevents metric drift.

Silent failures include inner joins dropping install-only rows or missing-offer events, an accidental `RELY` constraint allowing unsafe optimizer assumptions, and overwriting prices in place. For many continuous sources, replace file scans with Auto Loader checkpoints and schema rescue, serialize Silver merges, and consider HLL sketches only when exact distinct counts become prohibitively expensive.

## AI Usage

OpenCode was used to profile the supplied CSVs, review an initial architecture proposal, and implement this repository. A proposal claiming 299 unpriced reward payments was rejected after direct profiling showed zero; the code keeps the metric as an audit field instead. The proposed append-only Bronze design was rejected because static source paths could not make it idempotent without additional file-tracking machinery; Bronze therefore uses current raw snapshots. Offers use immutable IDs because no business-effective timestamp is supplied, while profile SCD2 is retained because its values are explicitly mutable snapshot aggregates. No parallel agents or prior-session context were used.
