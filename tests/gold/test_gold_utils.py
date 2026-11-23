from datetime import date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from gold.tables.utils import aggregate_events, aggregate_installs, combine_daily_kpis

D9 = date(2026, 5, 9)
D10 = date(2026, 5, 10)
D11 = date(2026, 5, 11)
D12 = date(2026, 5, 12)

INSTALLS_DAILY_COLUMNS = "user_id string, install_date date, country string, platform string"
USER_INSTALL_COLUMNS = "user_id string, country string, platform string"
EVENT_COLUMNS = (
    "user_id string, offer_id string, event_name string, event_ts timestamp, event_date date"
)
OFFER_COLUMNS = "offer_id string, payout_eur decimal(10,2)"
EVENTS_BY_DAY_COLUMNS = (
    "report_date date, country string, platform string, dau_app_open long, "
    "dau_offer_view long, dau_offer_start long, dau_goal_reached long, "
    "dau_reward_paid long, reward_payouts long, reward_cost_eur decimal(18,2)"
)
INSTALLS_BY_DAY_COLUMNS = "report_date date, country string, platform string, installs long"
COMBINED_COLUMNS = [
    "report_date",
    "country",
    "platform",
    "installs",
    "dau_app_open",
    "dau_offer_view",
    "dau_offer_start",
    "dau_goal_reached",
    "dau_reward_paid",
    "reward_payouts",
    "reward_cost_eur",
]
EVENT_METRICS = [
    "dau_app_open",
    "dau_offer_view",
    "dau_offer_start",
    "dau_goal_reached",
    "dau_reward_paid",
    "reward_payouts",
    "reward_cost_eur",
]


def day(value):
    return F.lit(value.isoformat()).cast("date")


def event(user, offer, name, when):
    return (user, offer, name, datetime(when.year, when.month, when.day, 12), when)


def rows_of(df, columns):
    return [tuple(row[c] for c in columns) for row in df.orderBy(*columns).collect()]


def event_by_day(report_date, country, platform, **metrics):
    values = {metric: 0 for metric in EVENT_METRICS}
    values["reward_cost_eur"] = Decimal("0.00")
    values.update(metrics)
    return (report_date, country, platform, *[values[m] for m in EVENT_METRICS])


# aggregate_installs


@pytest.mark.parametrize(
    ("rows", "start", "end", "expected"),
    [
        pytest.param(
            [("u-1", D10, "DE", "android"), ("u-2", D10, "DE", "android")],
            D10,
            D10,
            [(D10, "DE", "android", 2)],
            id="counts_users_in_the_same_group",
        ),
        pytest.param(
            [("u-1", D10, "DE", "android"), ("u-2", D10, "FR", "android")],
            D10,
            D10,
            [(D10, "DE", "android", 1), (D10, "FR", "android", 1)],
            id="splits_groups_by_country",
        ),
        pytest.param(
            [("u-1", D10, "DE", "android"), ("u-2", D10, "DE", "ios")],
            D10,
            D10,
            [(D10, "DE", "android", 1), (D10, "DE", "ios", 1)],
            id="splits_groups_by_platform",
        ),
        pytest.param(
            [("u-1", D10, "DE", "android"), ("u-2", D11, "DE", "android")],
            D10,
            D11,
            [(D10, "DE", "android", 1), (D11, "DE", "android", 1)],
            id="splits_groups_by_report_date",
        ),
        pytest.param(
            [
                ("u-1", D9, "DE", "android"),
                ("u-2", D10, "DE", "android"),
                ("u-3", D11, "DE", "android"),
                ("u-4", D12, "DE", "android"),
            ],
            D10,
            D11,
            [(D10, "DE", "android", 1), (D11, "DE", "android", 1)],
            id="includes_both_window_boundaries_and_excludes_outside_days",
        ),
        pytest.param(
            [("u-1", D10, None, "android"), ("u-2", D10, "DE", "android")],
            D10,
            D10,
            [(D10, None, "android", 1), (D10, "DE", "android", 1)],
            id="keeps_null_country_as_its_own_group",
        ),
        pytest.param(
            [("u-1", D9, "DE", "android")],
            D10,
            D10,
            [],
            id="returns_no_rows_when_nothing_is_in_the_window",
        ),
    ],
)
def test_aggregate_installs_groups_and_counts_users(spark, rows, start, end, expected):
    installs = spark.createDataFrame(rows, INSTALLS_DAILY_COLUMNS)

    actual = aggregate_installs(installs, day(start), day(end))

    assert rows_of(actual, ["report_date", "country", "platform", "installs"]) == expected


# aggregate_events: distinct users per event type


@pytest.mark.parametrize(
    ("event_name", "metric"),
    [
        ("app_open", "dau_app_open"),
        ("offer_view", "dau_offer_view"),
        ("offer_start", "dau_offer_start"),
        ("goal_reached", "dau_goal_reached"),
        ("reward_paid", "dau_reward_paid"),
    ],
)
def test_aggregate_events_counts_distinct_users_per_event_type(spark, event_name, metric):
    other_name = "offer_view" if event_name == "app_open" else "app_open"
    installs = spark.createDataFrame(
        [("u-1", "DE", "android"), ("u-2", "DE", "android"), ("u-3", "DE", "android")],
        USER_INSTALL_COLUMNS,
    )
    events = spark.createDataFrame(
        [
            event("u-1", "of-1", event_name, D10),
            event("u-1", "of-1", event_name, D10),
            event("u-2", "of-1", event_name, D10),
            event("u-3", "of-1", other_name, D10),
        ],
        EVENT_COLUMNS,
    )
    offers = spark.createDataFrame([("of-1", Decimal("1.50"))], OFFER_COLUMNS)

    actual = aggregate_events(events, installs, offers, day(D10), day(D10))

    assert rows_of(actual, [metric]) == [(2,)]


# aggregate_events: reward payouts and cost (these use the offers join)


@pytest.mark.parametrize(
    ("events", "offers", "metric", "expected"),
    [
        pytest.param(
            [
                event("u-1", "of-1", "reward_paid", D10),
                event("u-1", "of-1", "reward_paid", D10),
                event("u-2", "of-1", "reward_paid", D10),
                event("u-3", "of-1", "app_open", D10),
            ],
            [("of-1", Decimal("1.50"))],
            "reward_payouts",
            [(3,)],
            id="payouts_count_each_payment_not_each_user",
        ),
        pytest.param(
            [
                event("u-1", "of-1", "reward_paid", D10),
                event("u-2", "of-2", "reward_paid", D10),
                event("u-3", "of-1", "app_open", D10),
            ],
            [("of-1", Decimal("1.50")), ("of-2", Decimal("2.25"))],
            "reward_cost_eur",
            [(Decimal("3.75"),)],
            id="cost_sums_the_offer_payout_of_each_payment",
        ),
        pytest.param(
            [
                event("u-1", "of-1", "reward_paid", D10),
                event("u-2", "missing", "reward_paid", D10),
            ],
            [("of-1", Decimal("1.50"))],
            "reward_cost_eur",
            [(Decimal("1.50"),)],
            id="cost_skips_payments_whose_offer_is_missing",
        ),
        pytest.param(
            [
                event("u-1", "of-1", "reward_paid", D10),
                event("u-2", "missing", "reward_paid", D10),
            ],
            [("of-1", Decimal("1.50"))],
            "reward_payouts",
            [(2,)],
            id="payouts_still_count_payments_whose_offer_is_missing",
        ),
        pytest.param(
            [event("u-1", "of-1", "app_open", D10)],
            [("of-1", Decimal("1.50"))],
            "reward_payouts",
            [(0,)],
            id="payouts_are_zero_when_the_day_has_no_payments",
        ),
        pytest.param(
            [event("u-1", "of-1", "app_open", D10)],
            [("of-1", Decimal("1.50"))],
            "reward_cost_eur",
            [(None,)],
            id="cost_is_null_when_the_day_has_no_payments",
        ),
    ],
)
def test_aggregate_events_reward_payouts_and_cost(spark, events, offers, metric, expected):
    installs = spark.createDataFrame(
        [("u-1", "DE", "android"), ("u-2", "DE", "android"), ("u-3", "DE", "android")],
        USER_INSTALL_COLUMNS,
    )
    events_df = spark.createDataFrame(events, EVENT_COLUMNS)
    offers_df = spark.createDataFrame(offers, OFFER_COLUMNS)

    actual = aggregate_events(events_df, installs, offers_df, day(D10), day(D10))

    assert rows_of(actual, [metric]) == expected


# aggregate_events: grouping from the installing user, window, and missing installs


@pytest.mark.parametrize(
    ("events", "installs", "start", "end", "expected"),
    [
        pytest.param(
            [event("u-1", "of-1", "app_open", D10), event("u-2", "of-1", "app_open", D10)],
            [("u-1", "DE", "android"), ("u-2", "FR", "ios")],
            D10,
            D10,
            [(D10, "DE", "android", 1), (D10, "FR", "ios", 1)],
            id="groups_by_the_installing_users_country_and_platform",
        ),
        pytest.param(
            [event("u-1", "of-1", "app_open", D10), event("u-1", "of-1", "app_open", D11)],
            [("u-1", "DE", "android")],
            D10,
            D11,
            [(D10, "DE", "android", 1), (D11, "DE", "android", 1)],
            id="groups_by_event_date",
        ),
        pytest.param(
            [event("u-9", "of-1", "app_open", D10), event("u-1", "of-1", "app_open", D10)],
            [("u-1", "DE", "android")],
            D10,
            D10,
            [(D10, None, None, 1), (D10, "DE", "android", 1)],
            id="keeps_users_without_an_install_with_null_dimensions",
        ),
        pytest.param(
            [
                event("u-1", "of-1", "app_open", D9),
                event("u-1", "of-1", "app_open", D10),
                event("u-1", "of-1", "app_open", D11),
                event("u-1", "of-1", "app_open", D12),
            ],
            [("u-1", "DE", "android")],
            D10,
            D11,
            [(D10, "DE", "android", 1), (D11, "DE", "android", 1)],
            id="excludes_events_outside_the_window",
        ),
    ],
)
def test_aggregate_events_groups_by_install_dimensions_and_window(
    spark, events, installs, start, end, expected
):
    installs_df = spark.createDataFrame(installs, USER_INSTALL_COLUMNS)
    events_df = spark.createDataFrame(events, EVENT_COLUMNS)
    offers_df = spark.createDataFrame([("of-1", Decimal("1.50"))], OFFER_COLUMNS)

    actual = aggregate_events(events_df, installs_df, offers_df, day(start), day(end))

    assert rows_of(actual, ["report_date", "country", "platform", "dau_app_open"]) == expected


# combine_daily_kpis


@pytest.mark.parametrize(
    ("installs_by_day", "events_by_day", "expected"),
    [
        pytest.param(
            [(D10, "DE", "android", 2)],
            [],
            [(D10, "DE", "android", 2, 0, 0, 0, 0, 0, 0, Decimal("0.00"))],
            id="keeps_install_only_group_with_zero_event_metrics",
        ),
        pytest.param(
            [],
            [
                event_by_day(
                    D10,
                    "FR",
                    "ios",
                    dau_app_open=3,
                    reward_payouts=1,
                    reward_cost_eur=Decimal("1.50"),
                )
            ],
            [(D10, "FR", "ios", 0, 3, 0, 0, 0, 0, 1, Decimal("1.50"))],
            id="keeps_event_only_group_with_zero_installs",
        ),
        pytest.param(
            [(D10, "DE", "android", 2)],
            [
                event_by_day(
                    D10,
                    "DE",
                    "android",
                    dau_app_open=1,
                    dau_reward_paid=1,
                    reward_payouts=1,
                    reward_cost_eur=Decimal("1.50"),
                )
            ],
            [(D10, "DE", "android", 2, 1, 0, 0, 0, 1, 1, Decimal("1.50"))],
            id="joins_a_matching_group_into_one_row",
        ),
        pytest.param(
            [(D10, "DE", "android", 2)],
            [event_by_day(D10, "DE", "ios", dau_app_open=1)],
            [
                (D10, "DE", "android", 2, 0, 0, 0, 0, 0, 0, Decimal("0.00")),
                (D10, "DE", "ios", 0, 1, 0, 0, 0, 0, 0, Decimal("0.00")),
            ],
            id="keeps_different_platforms_as_separate_rows",
        ),
        pytest.param(
            [],
            [event_by_day(D10, None, None, dau_app_open=1)],
            [(D10, None, None, 0, 1, 0, 0, 0, 0, 0, Decimal("0.00"))],
            id="keeps_events_without_a_country_as_their_own_group",
        ),
    ],
)
def test_combine_daily_kpis_builds_one_row_per_group(
    spark, installs_by_day, events_by_day, expected
):
    installs_df = spark.createDataFrame(installs_by_day, INSTALLS_BY_DAY_COLUMNS)
    events_df = spark.createDataFrame(events_by_day, EVENTS_BY_DAY_COLUMNS)

    actual = combine_daily_kpis(installs_df, events_df)

    assert rows_of(actual, COMBINED_COLUMNS) == expected


@pytest.mark.parametrize("column", EVENT_METRICS)
def test_combine_daily_kpis_fills_a_missing_event_metric_with_zero(spark, column):
    installs_df = spark.createDataFrame([(D10, "DE", "android", 2)], INSTALLS_BY_DAY_COLUMNS)
    events_df = spark.createDataFrame([], EVENTS_BY_DAY_COLUMNS)
    zero = Decimal("0.00") if column == "reward_cost_eur" else 0

    row = combine_daily_kpis(installs_df, events_df).first()

    assert row[column] == zero
