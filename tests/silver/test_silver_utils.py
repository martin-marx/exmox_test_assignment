from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from silver.utils import (
    from_ingest_watermark,
    merge_column_mappings,
    normalize_country,
)


def test_normalize_country_removes_split_aggregation_values(spark):
    rows = spark.createDataFrame([("de ",), ("US",), (None,)], ["country"])
    normalized = rows.select(normalize_country(F.col("country")).alias("country")).collect()
    assert [row.country for row in normalized] == ["DE", "US", None]


def test_merge_column_mappings_preserve_the_source_column_names():
    assert merge_column_mappings(["user_id", "country"]) == {
        "user_id": "source.user_id",
        "country": "source.country",
    }


def test_event_deduplication_keeps_earliest_ingest_not_first_input_row(spark):
    events = spark.createDataFrame(
        [
            ("event-1", "2026-04-02 01:00:00", "wrong"),
            ("event-1", "2026-04-02 00:01:00", "right"),
        ],
        ["event_id", "ingest_ts", "event_name"],
    )
    actual = (
        events.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("event_id").orderBy(F.to_timestamp("ingest_ts").asc())
            ),
        )
        .where("rank = 1")
        .select("event_name")
        .first()
    )
    assert actual.event_name == "right"


def test_event_watermark_keeps_the_boundary_and_newer_arrivals(spark):
    events = spark.createDataFrame(
        [
            ("2026-04-02 00:00:00", "before"),
            ("2026-04-02 01:00:00", "boundary"),
            ("2026-04-02 02:00:00", "after"),
        ],
        ["ingest_ts", "event_name"],
    ).withColumn("_parsed_ingest_ts", F.to_timestamp("ingest_ts"))

    actual = from_ingest_watermark(events, datetime(2026, 4, 2, 1)).select("event_name").collect()

    assert [row.event_name for row in actual] == ["boundary", "after"]


def test_existing_event_id_with_changed_business_payload_is_a_contract_violation(spark):
    columns = ["event_id", "user_id", "event_ts", "event_name", "offer_id"]
    incoming = spark.createDataFrame(
        [("event-1", "user-1", "2026-05-10 12:00:00", "offer_view", "of-1")], columns
    )
    existing = spark.createDataFrame(
        [("event-1", "user-1", "2026-05-10 12:00:00", "offer_start", "of-1")], columns
    )

    conflicts = (
        incoming.alias("source")
        .join(existing.alias("target"), "event_id")
        .where(F.col("source.event_name") != F.col("target.event_name"))
    )

    assert conflicts.count() == 1


def test_offer_source_duplicate_ids_violate_its_grain(spark):
    offers = spark.createDataFrame(
        [("of-1", "casual", "cpi", 1.0), ("of-1", "casual", "cpi", 1.0)],
        ["offer_id", "offer_category", "payout_type", "payout_eur"],
    )

    assert offers.count() - offers.select("offer_id").distinct().count() == 1


def test_immutable_offer_id_conflicts_have_multiple_definitions(spark):
    offers = spark.createDataFrame(
        [("of-1", "casual", "cpi", 1.0), ("of-1", "puzzle", "cpi", 1.0)],
        ["offer_id", "offer_category", "payout_type", "payout_eur"],
    )

    conflicts = (
        offers.groupBy("offer_id")
        .agg(
            F.countDistinct(F.struct("offer_category", "payout_type", "payout_eur")).alias("count")
        )
        .where("count > 1")
    )

    assert conflicts.count() == 1


def test_rows_without_keys_or_parseable_timestamps_are_excluded_from_typed_events(spark):
    events = spark.createDataFrame(
        [
            ("event-1", "user-1", "2026-04-02 00:01:00", "2026-04-02 00:02:00"),
            ("", "user-2", "2026-04-02 00:01:00", "2026-04-02 00:02:00"),
            ("event-3", "user-3", "not-a-timestamp", "2026-04-02 00:02:00"),
        ],
        ["event_id", "user_id", "event_ts", "ingest_ts"],
    )
    typed_events = (
        events.withColumn("parsed_event_ts", F.try_to_timestamp("event_ts"))
        .withColumn("parsed_ingest_ts", F.try_to_timestamp("ingest_ts"))
        .where(F.trim("event_id") != "")
        .where(F.trim("user_id") != "")
        .where(F.col("parsed_event_ts").isNotNull() & F.col("parsed_ingest_ts").isNotNull())
    )
    assert typed_events.count() == 1
