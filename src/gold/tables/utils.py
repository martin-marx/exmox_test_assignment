"""Pure PySpark transformations for daily Gold KPI construction."""

from pyspark.sql import functions as F


def aggregate_installs(installs, start, end):
    return (
        installs.where(F.col("install_date").between(start, end))
        .groupBy(F.col("install_date").alias("report_date"), "country", "platform")
        .agg(F.count("user_id").alias("installs"))
    )


def aggregate_events(events, installs, offers, start, end):
    events_with_dimensions = (
        events.where(F.col("event_date").between(start, end))
        .alias("events")
        .join(
            installs.select("user_id", "country", "platform").alias("installs"),
            F.col("events.user_id") == F.col("installs.user_id"),
            "left",
        )
        .join(
            offers.alias("offers"),
            F.col("events.offer_id") == F.col("offers.offer_id"),
            "left",
        )
    )
    return events_with_dimensions.groupBy(
        F.col("events.event_date").alias("report_date"),
        F.col("installs.country").alias("country"),
        F.col("installs.platform").alias("platform"),
    ).agg(
        F.countDistinct(
            F.when(F.col("events.event_name") == "app_open", F.col("events.user_id"))
        ).alias("dau_app_open"),
        F.countDistinct(
            F.when(F.col("events.event_name") == "offer_view", F.col("events.user_id"))
        ).alias("dau_offer_view"),
        F.countDistinct(
            F.when(F.col("events.event_name") == "offer_start", F.col("events.user_id"))
        ).alias("dau_offer_start"),
        F.countDistinct(
            F.when(F.col("events.event_name") == "goal_reached", F.col("events.user_id"))
        ).alias("dau_goal_reached"),
        F.countDistinct(
            F.when(F.col("events.event_name") == "reward_paid", F.col("events.user_id"))
        ).alias("dau_reward_paid"),
        F.count(F.when(F.col("events.event_name") == "reward_paid", F.lit(1))).alias(
            "reward_payouts"
        ),
        F.sum(F.when(F.col("events.event_name") == "reward_paid", F.col("offers.payout_eur")))
        .cast("decimal(18,2)")
        .alias("reward_cost_eur"),
    )


def combine_daily_kpis(installs_by_day, events_by_day):
    return (
        installs_by_day.alias("installs")
        .join(
            events_by_day.alias("events"),
            (F.col("installs.report_date") == F.col("events.report_date"))
            & (F.col("installs.country") == F.col("events.country"))
            & (F.col("installs.platform") == F.col("events.platform")),
            "full",
        )
        .select(
            F.coalesce(F.col("installs.report_date"), F.col("events.report_date")).alias(
                "report_date"
            ),
            F.coalesce(F.col("installs.country"), F.col("events.country")).alias("country"),
            F.coalesce(F.col("installs.platform"), F.col("events.platform")).alias("platform"),
            F.coalesce(F.col("installs.installs"), F.lit(0)).alias("installs"),
            F.coalesce(F.col("events.dau_app_open"), F.lit(0)).alias("dau_app_open"),
            F.coalesce(F.col("events.dau_offer_view"), F.lit(0)).alias("dau_offer_view"),
            F.coalesce(F.col("events.dau_offer_start"), F.lit(0)).alias("dau_offer_start"),
            F.coalesce(F.col("events.dau_goal_reached"), F.lit(0)).alias("dau_goal_reached"),
            F.coalesce(F.col("events.dau_reward_paid"), F.lit(0)).alias("dau_reward_paid"),
            F.coalesce(F.col("events.reward_payouts"), F.lit(0)).alias("reward_payouts"),
            F.coalesce(F.col("events.reward_cost_eur"), F.lit(0).cast("decimal(18,2)")).alias(
                "reward_cost_eur"
            ),
        )
    )
