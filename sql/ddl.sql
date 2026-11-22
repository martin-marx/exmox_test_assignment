-- Run against a Unity Catalog-enabled SQL warehouse before the Workflow.
CREATE SCHEMA IF NOT EXISTS workspace.bronze;
CREATE SCHEMA IF NOT EXISTS workspace.silver;
CREATE SCHEMA IF NOT EXISTS workspace.gold;
CREATE VOLUME IF NOT EXISTS workspace.default.raw_data;

CREATE TABLE IF NOT EXISTS workspace.bronze.installs (
  user_id STRING, install_ts STRING, country STRING, platform STRING,
  media_source STRING, device_model STRING, campaign_id STRING
) COMMENT 'Raw install snapshot; all source fields are strings.';

CREATE TABLE IF NOT EXISTS workspace.bronze.events (
  user_id STRING, event_ts STRING, event_name STRING, offer_id STRING,
  event_id STRING, ingest_ts STRING
) COMMENT 'Raw event snapshot; source retries are intentionally retained.';

CREATE TABLE IF NOT EXISTS workspace.bronze.offers (
  offer_id STRING, offer_category STRING, payout_type STRING, payout_eur STRING
) COMMENT 'Raw offer snapshot.';

CREATE TABLE IF NOT EXISTS workspace.bronze.user_profile (
  user_id STRING, events_lifetime STRING, last_seen_ts STRING, revenue_30d_eur STRING,
  is_payer STRING
) COMMENT 'Raw backend profile snapshot for reconciliation.';

CREATE TABLE IF NOT EXISTS workspace.silver.installs (
  user_id STRING NOT NULL,
  install_ts TIMESTAMP,
  install_date DATE,
  country STRING,
  platform STRING,
  media_source STRING,
  device_model STRING,
  campaign_id STRING
) CLUSTER BY (install_date, user_id);

CREATE TABLE IF NOT EXISTS workspace.silver.offers (
  offer_id STRING NOT NULL,
  offer_category STRING,
  payout_type STRING,
  payout_eur DECIMAL(10, 2)
) CLUSTER BY (offer_id);

-- CREATE TABLE rejects CHECK constraints on this endpoint, so add the rule afterwards.
-- Re-running fails here once the constraint exists; the setup script treats that as an error.
ALTER TABLE workspace.silver.offers ADD CONSTRAINT offers_payout_non_negative CHECK (payout_eur >= 0);

CREATE TABLE IF NOT EXISTS workspace.silver.fact_events (
  event_id STRING NOT NULL,
  user_id STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  event_date DATE,
  event_name STRING,
  offer_id STRING
) CLUSTER BY (event_date, user_id);

CREATE TABLE IF NOT EXISTS workspace.silver.user_profile (
  user_id STRING NOT NULL,
  events_lifetime BIGINT,
  last_seen_ts TIMESTAMP,
  revenue_30d_eur DECIMAL(10, 2),
  is_payer BOOLEAN,
  valid_from TIMESTAMP NOT NULL,
  valid_to TIMESTAMP,
  is_current BOOLEAN NOT NULL
) CLUSTER BY (user_id);

CREATE TABLE IF NOT EXISTS workspace.gold.daily_kpis (
  report_date DATE NOT NULL,
  country STRING,
  platform STRING,
  installs BIGINT NOT NULL,
  dau_app_open BIGINT NOT NULL,
  dau_offer_view BIGINT NOT NULL,
  dau_offer_start BIGINT NOT NULL,
  dau_goal_reached BIGINT NOT NULL,
  dau_reward_paid BIGINT NOT NULL,
  reward_payouts BIGINT NOT NULL,
  reward_cost_eur DECIMAL(18, 2) NOT NULL,
  unpriced_reward_payouts BIGINT NOT NULL
) CLUSTER BY (report_date, country, platform);
