import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pipeline.validation import validate_and_split
from pipeline.transformations import apply_watermark_and_dedup, compute_windowed_aggregates
from schemas.contracts import RAW_TRANSACTION_SCHEMA

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("UnitTests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "1") \
        .getOrCreate()

def test_empty_batch_handling(spark):
    empty_df = spark.createDataFrame([], RAW_TRANSACTION_SCHEMA)
    clean_df, reject_df = validate_and_split(empty_df)
    assert clean_df.count() == 0
    assert reject_df.count() == 0

def test_validation_rejects_malformed_and_domain_edge_cases(spark):
    test_rows = [
        Row(transaction_id="TX_1", account_id="ACC_1", merchant_id="MCH_001", amount=100.0, currency="USD", event_time="2026-11-12T10:00:00Z"),
        Row(transaction_id=None, account_id="ACC_2", merchant_id="MCH_001", amount=100.0, currency="USD", event_time="2026-11-12T10:00:00Z"),
        Row(transaction_id="TX_3", account_id="ACC_3", merchant_id="MCH_001", amount=-20.0, currency="USD", event_time="2026-11-12T10:00:00Z"),
        Row(transaction_id="TX_4", account_id="ACC_4", merchant_id="UNKNOWN_MCH", amount=50.0, currency="USD", event_time="2026-11-12T10:00:00Z"),
        Row(transaction_id="TX_5", account_id="ACC_5", merchant_id="MCH_002", amount=50.0, currency="USD", event_time="NOT_A_TIMESTAMP"),
    ]
    df = spark.createDataFrame(test_rows, RAW_TRANSACTION_SCHEMA)
    clean_df, reject_df = validate_and_split(df)

    assert clean_df.count() == 1
    assert reject_df.count() == 4

def test_deduplication_and_window_aggregation(spark):
    test_rows = [
        Row(transaction_id="DUP_1", account_id="ACC_1", merchant_id="MCH_001", amount=100.0, currency="USD", event_time="2026-11-12T10:01:00Z"),
        Row(transaction_id="DUP_1", account_id="ACC_1", merchant_id="MCH_001", amount=100.0, currency="USD", event_time="2026-11-12T10:01:00Z"),
        Row(transaction_id="TX_2", account_id="ACC_2", merchant_id="MCH_001", amount=50.0, currency="USD", event_time="2026-11-12T10:02:00Z"),
    ]
    df = spark.createDataFrame(test_rows, RAW_TRANSACTION_SCHEMA)
    clean_df, _ = validate_and_split(df)
    deduped = apply_watermark_and_dedup(clean_df, "10 minutes")
    assert deduped.count() == 2

    agg = compute_windowed_aggregates(deduped, "5 minutes").collect()
    assert len(agg) == 1
    assert agg[0]["tx_count"] == 2
    assert agg[0]["total_volume"] == 150.0
