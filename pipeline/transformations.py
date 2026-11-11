from pyspark.sql import DataFrame
from pyspark.sql.functions import col, window, sum as _sum, count as _count

def apply_watermark_and_dedup(df: DataFrame, watermark_delay: str = "10 minutes") -> DataFrame:
    return df \
        .withWatermark("event_time", watermark_delay) \
        .dropDuplicates(["transaction_id", "event_time"])

def compute_windowed_aggregates(df: DataFrame, window_duration: str = "5 minutes") -> DataFrame:
    return df \
        .groupBy(
            window(col("event_time"), window_duration),
            col("merchant_id")
        ) \
        .agg(
            _count("transaction_id").alias("tx_count"),
            _sum("amount").alias("total_volume")
        )
