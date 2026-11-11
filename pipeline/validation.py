from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, concat_ws, lit, to_timestamp, current_timestamp
from schemas.contracts import VALID_MERCHANTS

def validate_and_split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    df_parsed = df.withColumn("parsed_event_time", to_timestamp(col("event_time")))

    null_id = col("transaction_id").isNull()
    null_event_time = col("parsed_event_time").isNull()
    invalid_amount = col("amount").isNull() | (col("amount") <= 0.0)
    invalid_merchant = ~col("merchant_id").isin(list(VALID_MERCHANTS)) | col("merchant_id").isNull()

    df_flagged = df_parsed.withColumn(
        "rejection_reasons",
        concat_ws(" | ",
            when(null_id, lit("NULL_TRANSACTION_ID")),
            when(null_event_time, lit("INVALID_OR_MISSING_TIMESTAMP")),
            when(invalid_amount, lit("INVALID_AMOUNT_VALUE")),
            when(invalid_merchant, lit("FOREIGN_KEY_VIOLATION_UNKNOWN_MERCHANT"))
        )
    )

    clean_df = df_flagged.filter(col("rejection_reasons") == "") \
        .drop("rejection_reasons", "event_time") \
        .withColumnRenamed("parsed_event_time", "event_time") \
        .withColumn("ingest_time", current_timestamp())

    reject_df = df_flagged.filter(col("rejection_reasons") != "") \
        .select(
            col("transaction_id"),
            col("rejection_reasons").alias("failure_reason"),
            current_timestamp().alias("quarantine_time"),
            col("event_time").alias("raw_event_time"),
            col("amount").alias("raw_amount")
        )

    return clean_df, reject_df
