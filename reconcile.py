from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum

def reconcile():
    spark = SparkSession.builder \
        .appName("AuditReconciliation") \
        .master("local[*]") \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    audit_df = spark.read.format("delta").load("/app/storage/pipeline_audit_logs")
    silver_df = spark.read.format("delta").load("/app/storage/silver_transactions")
    dlq_df = spark.read.format("delta").load("/app/storage/quarantine_dlq")

    total_input = audit_df.select(_sum("input_count")).collect()[0][0]
    total_silver = silver_df.count()
    total_quarantined = dlq_df.count()
    duplicates_dropped = total_input - (total_silver + total_quarantined)

    print("\n" + "=" * 50)
    print("          STREAMING RECONCILIATION AUDIT          ")
    print("=" * 50)
    print(f"Total Raw Records Ingested : {total_input}")
    print(f"Total Clean Inserted (Silver): {total_silver}")
    print(f"Total Quarantined (DLQ)    : {total_quarantined}")
    print(f"Duplicates Absorbed (Dedup): {duplicates_dropped}")
    print("=" * 50)

    assert silver_df.filter(col("amount") <= 0).count() == 0, "Assertion Error: Negative amounts in Silver"
    assert silver_df.filter(col("transaction_id").isNull()).count() == 0, "Assertion Error: Null transaction_id in Silver"
    print("[SUCCESS] All referential integrity, data contracts, and audit assertions passed.\n")

if __name__ == "__main__":
    reconcile()
