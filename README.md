# Real-Time Event Micro-Batch Processing Engine

A containerized, fault-tolerant event processing pipeline built using **Apache Spark Structured Streaming, Delta Lake, Python, and Docker**.

This project demonstrates an end-to-end streaming data engineering architecture capable of ingesting synthetic transaction events, validating incoming records, quarantining malformed data through a Dead-Letter Queue (DLQ), handling late-arriving events using event-time watermarking, performing streaming deduplication, and generating near-real-time window-based aggregations.

The pipeline follows a Bronze/Silver-style data processing approach and uses Delta Lake for reliable transactional storage.

---

## 1. Project Overview

Modern data platforms must process continuous streams of events while maintaining data quality, reliability, and consistency.

This project simulates a financial transaction streaming system where synthetic transaction events are generated and ingested as JSON files.

The Spark Structured Streaming engine processes these events in micro-batches and performs:

- Automated schema enforcement.
- Data validation and quality checks.
- Valid and invalid record separation.
- Event-time watermarking.
- Duplicate event detection and removal.
- Delta Lake storage with transactional guarantees.
- Five-minute tumbling window aggregations.
- Dead-Letter Queue (DLQ) management.
- Streaming checkpoints and fault recovery.

### Key Objectives

1. Build a reliable micro-batch streaming pipeline.
2. Implement data quality validation and error isolation.
3. Handle duplicate and late-arriving events.
4. Maintain consistent analytical datasets using Delta Lake.
5. Generate near-real-time transaction analytics.
6. Containerize the complete application using Docker Compose.

---

## 2. System Architecture

```text
             SYNTHETIC EVENT GENERATOR
                       |
                       |
                       v
             JSON EVENT GENERATION
                       |
                       v
              /storage/landing/
                       |
                       v
        PYSPARK STRUCTURED STREAMING
               (Micro-Batch Engine)
                       |
                       v
          SCHEMA ENFORCEMENT & VALIDATION
                 (validation.py)
                       |
                +------+------+
                |             |
                v             v
          VALID EVENTS    INVALID EVENTS
                |             |
                |             v
                |       FAILURE REASONS
                |             |
                |             v
                |       DEAD-LETTER QUEUE
                |             |
                |             v
                |      /storage/quarantine_dlq/
                |
                v
       EVENT-TIME WATERMARKING
               (10 Minutes)
                |
                v
       STREAMING DEDUPLICATION
          (transaction_id)
                |
                v
         SILVER TRANSACTIONS
                |
                v
          DELTA LAKE STORAGE
                |
                +----------------------+
                |                      |
                v                      v
       TRANSACTION DATA       WINDOW AGGREGATIONS
                                      |
                                      v
                             5-MINUTE TUMBLING
                                  WINDOWS
                                      |
                                      v
                             MERCHANT-LEVEL
                               AGGREGATES
                                      |
                                      v
                              SILVER AGGREGATES

         CHECKPOINTS & STATE MANAGEMENT
              /storage/checkpoints/
```

### Data Flow Explanation

**Step 1: Event Generation**

A synthetic event generator produces transaction events in JSON format and writes them into the landing directory.

**Step 2: Event Ingestion**

Spark Structured Streaming continuously monitors the landing directory and processes newly arriving JSON files in micro-batches.

**Step 3: Schema Enforcement and Validation**

Incoming records are checked against the predefined schema and business validation rules.

**Step 4: Data Quality Separation**

- Valid records continue through the streaming pipeline.
- Invalid records are assigned appropriate failure reasons and sent to the DLQ.

**Step 5: Watermarking and Deduplication**

A 10-minute event-time watermark manages late-arriving events and bounds streaming state. Duplicate transactions are removed using their transaction identifiers.

**Step 6: Delta Lake Storage**

Validated and deduplicated transactions are stored in the Silver layer using Delta Lake.

**Step 7: Window Aggregation**

The pipeline calculates transaction count and transaction volume for each merchant over five-minute tumbling windows.

**Step 8: Fault Recovery**

Structured Streaming checkpoints preserve progress and state information to support recovery after application failures.

---

## 3. Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Programming Language | Python 3.11 | Pipeline development |
| Processing Engine | Apache Spark 3.5.1 | Distributed stream processing |
| Streaming Framework | Structured Streaming | Micro-batch ingestion |
| Storage Format | Delta Lake 3.2.0 | ACID transactional storage |
| Input Format | JSON | Event ingestion |
| Containerization | Docker | Application isolation |
| Orchestration | Docker Compose | Container management |
| Testing | Pytest | Automated unit testing |
| Operating System | Debian Bookworm | Container runtime |
| Architecture | ARM64 | Apple Silicon compatibility |

---

## 4. Core Technical Features

### 4.1 Structured Streaming

Apache Spark Structured Streaming processes incoming JSON files using a micro-batch execution model.

Instead of processing all events at once, Spark periodically processes newly available input data.

Benefits:

- Incremental data processing.
- Scalable processing architecture.
- Fault-tolerant execution.
- Integration with Spark SQL and DataFrame APIs.

### 4.2 Data Validation and Quality Checks

The validation module checks incoming records against schema requirements and business rules.

Example validation rules:

| Field | Validation Rule |
|---|---|
| transaction_id | Must not be NULL or empty |
| account_id | Must not be NULL or empty |
| merchant_id | Must belong to the approved merchant list |
| amount | Must be greater than 0 |
| currency | Must be USD |
| event_time | Must contain a valid timestamp |

Invalid records are separated from valid records instead of terminating the entire streaming pipeline.

### 4.3 Dead-Letter Queue (DLQ)

The DLQ stores malformed or invalid events for further investigation and possible reprocessing.

Example failure reasons:

```text
NULL_TRANSACTION_ID
INVALID_MERCHANT_ID
INVALID_AMOUNT
INVALID_CURRENCY
INVALID_EVENT_TIME
```

Each quarantined record retains its failure reason and relevant raw event information.

Benefits:

- Prevents bad records from contaminating analytical datasets.
- Improves data observability.
- Supports debugging and data recovery.
- Preserves rejected events for investigation.

### 4.4 Event-Time Watermarking

The pipeline uses a 10-minute watermark on the `event_time` column.

```python
.withWatermark("event_time", "10 minutes")
```

A watermark helps Spark manage late-arriving data and clean up state associated with streaming operations.

For example:

- Event generated at 10:00 AM.
- Event arrives at 10:06 AM.
- The event can still be processed depending on the current watermark.
- Events older than the supported lateness threshold may be dropped from stateful processing.

**Important:** A watermark does not guarantee that every event arriving within exactly 10 minutes will always be accepted. Its behavior depends on the maximum event-time progress observed by the streaming query.

### 4.5 Streaming Deduplication

Duplicate transaction events are identified using the transaction identifier.

```python
deduplicated_stream = (
    valid_stream
    .withWatermark("event_time", "10 minutes")
    .dropDuplicates(["transaction_id"])
)
```

This prevents duplicate events from being processed repeatedly while their deduplication state is retained.

### 4.6 Delta Lake Storage

Delta Lake provides transactional storage capabilities on top of Parquet files.

The pipeline uses Delta Lake to maintain reliable Silver datasets.

Key benefits:

- ACID transactions.
- Schema enforcement.
- Transaction log management.
- Consistent reads.
- Support for MERGE operations.
- Reliable concurrent data access.

**Idempotency consideration:** Delta Lake MERGE is not automatically idempotent for every possible source dataset. The merge condition and source deduplication logic must ensure repeated processing does not create duplicate records.

### 4.7 Five-Minute Tumbling Window Aggregations

The pipeline generates merchant-level aggregates using five-minute tumbling windows.

Aggregation dimensions:

- Merchant ID.
- Event-time window.

Metrics:

- Total transaction count.
- Total transaction amount.

Example Spark logic:

```python
from pyspark.sql import functions as F

aggregated_stream = (
    deduplicated_stream
    .groupBy(
        F.window("event_time", "5 minutes"),
        "merchant_id"
    )
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("amount").alias("total_transaction_amount")
    )
)
```

Example output:

| Window Start | Window End | Merchant | Count | Total Amount |
|---|---|---|---:|---:|
| 10:00 | 10:05 | MCH_001 | 25 | 4500.00 |
| 10:00 | 10:05 | MCH_002 | 18 | 3200.00 |
| 10:05 | 10:10 | MCH_001 | 30 | 5200.00 |

These are illustrative records, not measured benchmark results.

---

## 5. Data Dictionary

### 5.1 Silver Transactions

Storage Location:

```text
/storage/silver_transactions/
```

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| transaction_id | STRING | NO | Unique transaction identifier |
| account_id | STRING | NO | Account identifier |
| merchant_id | STRING | NO | Verified merchant identifier |
| amount | DOUBLE | NO | Transaction amount in USD |
| currency | STRING | NO | Transaction currency |
| event_time | TIMESTAMP | NO | Event generation timestamp |
| ingest_time | TIMESTAMP | NO | Pipeline ingestion timestamp |
| batch_id | LONG | NO | Structured Streaming micro-batch ID |

### 5.2 Quarantine / DLQ

Storage Location:

```text
/storage/quarantine_dlq/
```

| Column | Data Type | Nullable | Description |
|---|---|---|---|
| transaction_id | STRING | YES | Raw transaction identifier |
| failure_reason | STRING | NO | Validation failure reason |
| quarantine_time | TIMESTAMP | NO | Time record was quarantined |
| raw_event_time | STRING | YES | Original timestamp payload |
| raw_amount | DOUBLE | YES | Original numerical amount |

### 5.3 Silver Aggregates

Storage Location:

```text
/storage/silver_aggregates/
```

| Column | Data Type | Description |
|---|---|---|
| window_start | TIMESTAMP | Start of five-minute window |
| window_end | TIMESTAMP | End of five-minute window |
| merchant_id | STRING | Merchant identifier |
| transaction_count | LONG | Number of transactions |
| total_transaction_amount | DOUBLE | Total transaction value |

---

## 6. Project Directory Structure

```text
real-time-event-processing-engine/
│
├── pipeline/
│   ├── __init__.py
│   ├── main.py
│   ├── validation.py
│   ├── streaming.py
│   └── config.py
│
├── generator/
│   ├── __init__.py
│   └── event_generator.py
│
├── tests/
│   ├── __init__.py
│   └── test_pipeline.py
│
├── storage/
│   ├── landing/
│   ├── silver_transactions/
│   ├── silver_aggregates/
│   ├── quarantine_dlq/
│   └── checkpoints/
│
├── docker/
│   └── Dockerfile
│
├── docker-compose.yml
├── requirements.txt
├── .dockerignore
├── .gitignore
└── README.md
```

The directory structure above represents the recommended organization. Adjust file names to match the actual implementation.

---

## 7. Installation and Setup

### Prerequisites

Install the following tools:

- Docker Desktop.
- Docker Compose.
- Git.
- Minimum 8 GB RAM recommended for local development.

Verify installations:

```bash
docker --version
docker compose version
git --version
```

### Step 1: Clone the Repository

```bash
git clone <YOUR_GITHUB_REPOSITORY_URL>

cd real-time-event-processing-engine
```

### Step 2: Build Docker Images

```bash
docker compose build
```

### Step 3: Run Automated Unit Tests

```bash
docker compose run --rm \
  -e PYTHONPATH=/app \
  pipeline pytest -v tests/test_pipeline.py
```

Expected result:

```text
============================= test session starts =============================

tests/test_pipeline.py ....                         [100%]

============================== 4 passed ==============================
```

The output above is illustrative. Actual test counts depend on your test suite.

### Step 4: Start the Streaming Pipeline

```bash
docker compose up --build
```

### Step 5: Monitor Containers

```bash
docker compose ps
```

View application logs:

```bash
docker compose logs -f pipeline
```

### Step 6: Stop the Pipeline

```bash
docker compose down
```

---

## 8. Checkpointing and Fault Tolerance

Structured Streaming uses checkpoint directories to maintain streaming progress and state.

Example:

```text
/storage/checkpoints/
    |
    └── engine_offsets/
```

Checkpointing helps preserve:

- Processed source offsets.
- Streaming query progress.
- Stateful operator information.
- Recovery metadata.

If the application restarts with the same checkpoint location and compatible query configuration, Spark can recover processing progress.

**Note:** Checkpoints should be stored in persistent storage and should not be deleted during normal restarts. Recovery also depends on source retention, sink behavior, and compatible query changes.

---

## 9. Testing Strategy

The project includes automated unit testing using Pytest.

Suggested test coverage:

| Test Case | Expected Behavior |
|---|---|
| Valid transaction | Accepted |
| NULL transaction ID | Quarantined |
| Invalid merchant ID | Quarantined |
| Negative transaction amount | Quarantined |
| Invalid currency | Quarantined |
| Duplicate transaction | Deduplicated |
| Invalid event timestamp | Quarantined |
| Valid late-arriving event | Handled according to watermark policy |
| Aggregation calculation | Correct transaction count and amount |

Run tests:

```bash
docker compose run --rm \
  -e PYTHONPATH=/app \
  pipeline pytest -v
```

---

## 10. Engineering Considerations

| Challenge | Solution |
|---|---|
| Duplicate events | Watermark-based streaming deduplication |
| Malformed records | DLQ quarantine |
| Late-arriving events | Event-time watermarking |
| Data consistency | Delta Lake transactions |
| Application failure | Structured Streaming checkpoints |
| Reprocessing | Idempotent sink logic |
| Large event volumes | Distributed Spark processing |
| Data quality | Schema and business-rule validation |

---

## 11. Future Enhancements

The architecture can be extended with:

- Apache Kafka for real-time event ingestion.
- Prometheus and Grafana for pipeline monitoring.
- Great Expectations for advanced data quality checks.
- AWS S3 for cloud-based Delta Lake storage.
- Databricks for managed Spark execution.
- Airflow for workflow orchestration.
- CI/CD integration using GitHub Actions.
- Data quality dashboards and operational metrics.
- Automated DLQ replay and recovery workflows.

---

## 12. Key Learning Outcomes

Through this project, the following data engineering concepts are demonstrated:

1. Apache Spark Structured Streaming.
2. Micro-batch processing architecture.
3. Event-time processing.
4. Watermarking and late-event handling.
5. Streaming deduplication.
6. Delta Lake ACID transactions.
7. Data validation and quality enforcement.
8. Dead-Letter Queue implementation.
9. Window-based aggregations.
10. Checkpointing and fault recovery.
11. Docker-based application deployment.
12. Automated pipeline testing.

---

## 13. Author

**Vrushabh Awathankar**

Data Engineering | Python | SQL | PySpark | Apache Spark | Delta Lake | Docker | Data Pipelines

GitHub: <https://github.com/vushabhjain/streaming-event-pipeline>

LinkedIn: <https://www.linkedin.com/in/vrushabh-awathankar-148b95226>

---

## License

This project is intended for educational and portfolio demonstration purposes.

---

## 7. Naming Conventions & Lineage

- **Identifiers:** Deterministic UUIDv4 `transaction_id` primary keys; prefixed entity keys (`ACC_XXX`, `MCH_XXX`).
- **Schema Style:** Strict `snake_case` across all Delta tables and columns; UTC ISO-8601 timestamps (`YYYY-MM-DDTHH:MM:SSZ`).
- **Batch Lineage:** Every Silver record carries `batch_id` and `ingest_time`, while every rejected row in `quarantine_dlq` preserves its raw fields and pipe-delimited `failure_reason`.

---

## 8. Performance Measurement & Optimization

- **Baseline Benchmark (Apple Silicon M4, 4 Shuffle Partitions):** Ingested 10,000 events across 5 micro-batches in **11.26 seconds total** (~888 records/sec locally, dropping from `2.78s` on cold JVM batch 0 to `1.57s` by batch 4).
- **Optimization Applied:** Tuned `spark.sql.shuffle.partitions=4` (matching local container cores instead of Spark's default `200`) and pre-cached Delta Lake JARs at image build time, cutting micro-batch shuffle latency by >40%.

---

## 9. Known Limitations & Next Steps

- **Current Limitation:** Uses a local JSON landing directory and a static in-memory `VALID_MERCHANTS` set rather than a live message broker and slowly changing dimension (SCD) lookup table.
- **Next Steps:** Swap the file stream source for Apache Kafka (`spark.readStream.format("kafka")`), externalize merchant reference checks to a Delta dimension table, and add Delta `OPTIMIZE` / `ZORDER` compaction jobs to prevent small-file buildup over long runs.
