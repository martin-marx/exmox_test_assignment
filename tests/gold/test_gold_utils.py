from datetime import date, datetime
from decimal import Decimal

from gold.utils import aggregate_events, aggregate_installs, combine_daily_kpis


def test_aggregate_installs_filters_the_reporting_window_and_counts_distinct_users(spark):
    installs = spark.createDataFrame(
        [
            ("u-1", date(2026, 5, 10), "DE", "android"),
            ("u-2", date(2026, 5, 10), "DE", "android"),
            ("u-2", date(2026, 5, 10), "DE", "android"),
            ("u-3", date(2026, 5, 9), "DE", "android"),
        ],
        "user_id string, install_date date, country string, platform string",
    )

    actual = aggregate_installs(installs, date(2026, 5, 10), date(2026, 5, 10)).first()

    assert actual.installs == 2
    assert actual.report_date == date(2026, 5, 10)


def test_aggregate_events_calculates_metrics_cost_and_unpriced_payouts(spark):
    installs = spark.createDataFrame(
        [("u-1", "DE", "android"), ("u-2", "DE", "android")],
        "user_id string, country string, platform string",
    )
    events = spark.createDataFrame(
        [
            ("u-1", "of-1", "app_open", datetime(2026, 5, 10, 9), date(2026, 5, 10)),
            ("u-1", "of-1", "app_open", datetime(2026, 5, 10, 10), date(2026, 5, 10)),
            ("u-2", "of-1", "reward_paid", datetime(2026, 5, 10, 11), date(2026, 5, 10)),
            ("u-2", "missing", "reward_paid", datetime(2026, 5, 10, 12), date(2026, 5, 10)),
        ],
        "user_id string, offer_id string, event_name string, event_ts timestamp, event_date date",
    )
    offers = spark.createDataFrame(
        [("of-1", Decimal("1.50"))], "offer_id string, payout_eur decimal(10,2)"
    )

    actual = aggregate_events(
        events, installs, offers, date(2026, 5, 10), date(2026, 5, 10)
    ).first()

    assert actual.dau_app_open == 1
    assert actual.dau_reward_paid == 1
    assert actual.reward_payouts == 2
    assert actual.reward_cost_eur == Decimal("1.50")
    assert actual.unpriced_reward_payouts == 1


def test_combine_daily_kpis_preserves_install_only_rows_and_coalesces_metrics(spark):
    installs_by_day = spark.createDataFrame(
        [(date(2026, 5, 10), "DE", "android", 2)],
        "report_date date, country string, platform string, installs long",
    )
    events_by_day = spark.createDataFrame(
        [],
        (
            "report_date date, country string, platform string, dau_app_open long, "
            "dau_offer_view long, dau_offer_start long, dau_goal_reached long, "
            "dau_reward_paid long, reward_payouts long, reward_cost_eur decimal(18,2), "
            "unpriced_reward_payouts long"
        ),
    )

    actual = combine_daily_kpis(installs_by_day, events_by_day).first()

    assert actual.installs == 2
    assert actual.dau_app_open == 0
    assert actual.reward_payouts == 0
    assert actual.reward_cost_eur == Decimal("0.00")
