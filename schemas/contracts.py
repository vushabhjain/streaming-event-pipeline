from pyspark.sql.types import StructType, StructField, StringType, DoubleType

RAW_TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("event_time", StringType(), True)
])

VALID_MERCHANTS = {"MCH_001", "MCH_002", "MCH_003", "MCH_004", "MCH_005"}
