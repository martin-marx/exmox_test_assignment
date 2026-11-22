import pytest
from pyspark.sql.types import StringType

from bronze.utils import ingest_source, parse_source_columns, source_path


def test_source_path_matches_the_static_exercise_file():
    assert source_path("workspace", "events") == "/Volumes/workspace/default/raw_data/events.csv"


def test_parse_source_columns_requires_a_json_array_of_strings():
    assert parse_source_columns('["event_id", "event_ts"]') == ["event_id", "event_ts"]
    with pytest.raises(ValueError, match="JSON array"):
        parse_source_columns('{"event_id": "string"}')


def test_ingest_source_returns_string_columns(spark, tmp_path):
    source_file = tmp_path / "installs.csv"
    source_file.write_text("user_id,install_ts\nu-1,2026-05-01 00:00:00\n")

    actual = ingest_source(spark, str(source_file), ["user_id", "install_ts"])

    assert isinstance(actual.schema["user_id"].dataType, StringType)
    assert isinstance(actual.schema["install_ts"].dataType, StringType)
    assert actual.first().user_id == "u-1"
