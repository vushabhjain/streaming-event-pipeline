import os, time
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from schemas.contracts import RAW_TRANSACTION_SCHEMA
from pipeline.validation import validate_and_split
from pipeline.transformations import compute_windowed_aggregates

def process_micro_batch(batch_df, batch_id: int):
    start_time = datetime.now(timezone.utc)
    input_count = batch_df.count()

    if input_count == 0:
        return

    spark = batch_df.sparkSession

    # 1. Validation & Quality Split
    clean_df, reject_df = validate_and_split(batch_df)
    clean_df = clean_df.withColumn("batch_id", lit(batch_id))

    # 2. In-batch Deduplication
    deduped_df = clean_df.dropDuplicates(["transaction_id"])
    clean_count = deduped_df.count()
    reject_count = reject_df.count()

    # 3. Idempotent Merge into Silver (Curated Lakehouse)
    silver_path = "/app/storage/silver_transactions"
    if not DeltaTable.isDeltaTable(spark, silver_path):
        deduped_df.write.format("delta").mode("append").save(silver_path)
    else:
        delta_silver = DeltaTable.forPath(spark, silver_path)
        delta_silver.alias("tgt").merge(
            deduped_df.alias("src"),
            "tgt.transaction_id = src.transaction_id"
        ).whenNotMatchedInsertAll().execute()

    # 4. Quarantine Rejects (DLQ)
    if reject_count > 0:
        reject_df.write.format("delta").mode("append").save("/app/storage/quarantine_dlq")

    # 5. Near-Real-Time Aggregate Upsert
    agg_df = compute_windowed_aggregates(deduped_df)
    agg_path = "/app/storage/silver_aggregates"
    if not DeltaTable.isDeltaTable(spark, agg_path):
        agg_df.write.format("delta").mode("append").save(agg_path)
    else:
        delta_agg = DeltaTable.forPath(spark, agg_path)
        delta_agg.alias("tgt").merge(
            agg_df.alias("src"),
            """
            tgt.window.start = src.window.start AND 
            tgt.window.end = src.window.end AND 
            tgt.merchant_id = src.merchant_id
            """
        ).whenMatchedUpdate(set={
            "tx_count": "tgt.tx_count + src.tx_count",
            "total_volume": "tgt.total_volume + src.total_volume"
        }).whenNotMatchedInsertAll().execute()

    # 6. Audit & Observability Tracking
    duration_sec = (datetime.now(timezone.utc) - start_time).total_seconds()
    audit_data = [(batch_id, str(start_time), input_count, clean_count, reject_count, duration_sec)]
    audit_df = spark.createDataFrame(
        audit_data, 
        ["batch_id", "start_time", "input_count", "clean_count", "reject_count", "duration_sec"]
    )
    audit_df.write.format("delta").mode("append").save("/app/storage/pipeline_audit_logs")
    print(f"[Batch {batch_id}] Ingested: {input_count} | Clean: {clean_count} | Rejects: {reject_count} | Runtime: {duration_sec:.2f}s", flush=True)

def run():
    spark = SparkSession.builder \
        .appName("MacM4StreamEngine") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    print(">>> Spark Structured Streaming Active. Watching /app/storage/landing ...", flush=True)

    stream = spark.readStream \
        .schema(RAW_TRANSACTION_SCHEMA) \
        .option("maxFilesPerTrigger", 1) \
        .json("/app/storage/landing/")

    query = stream.writeStream \
        .foreachBatch(process_micro_batch) \
        .option("checkpointLocation", "/app/storage/checkpoints/engine_offsets") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run()
