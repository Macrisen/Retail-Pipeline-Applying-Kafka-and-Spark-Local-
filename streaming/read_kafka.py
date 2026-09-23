"""Read Kafka for local inspection; no database writes or event deduplication.

Keep the same checkpoint when restarting against the same Kafka topic.
If the topic is deleted/recreated, use a new checkpoint directory for that test.
Batch summaries may repeat if a batch is interrupted before its commit.
"""

from pathlib import Path

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType


CHECKPOINT = (
    Path(__file__).resolve().parents[1]
    / ".local" / "checkpoints" / "retail-kafka-json-v1"
)

# Producer preserves CSV values as strings. Parse structure first; numeric/time
# conversion and business validation will be a separate transformation step.
EVENT_FIELDS = [
    "event_id", "order_id", "event_type", "event_time_utc", "producer_time_utc",
    "customer_id", "product_id", "category", "quantity", "unit_price_vnd",
    "order_total_vnd", "payment_method", "city", "device", "traffic_source",
    "promo_code", "is_late_event",
]
EVENT_SCHEMA = StructType([
    StructField(name, StringType(), True)
    for name in EVENT_FIELDS + ["_corrupt_record"]
])


def parse_events(messages):
    """Retain every Kafka record, its raw JSON and coordinates for inspection."""
    parsed = messages.withColumn(
        "event", F.from_json("event_json", EVENT_SCHEMA, {
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
            "allowSingleQuotes": "false",
        })
    )
    return parsed.select(
        "order_key", "event_json", "topic", "partition", "offset", "kafka_timestamp",
        "event.*",
        (F.col("event").isNull() | F.col("event._corrupt_record").isNotNull())
        .alias("json_parse_error"),
    )


def summarize_batch(batch, batch_id):
    # Aggregate this bounded batch only; do not print thousands of JSON payloads.
    summaries = (
        batch.groupBy("partition")
        .agg(
            F.count("*").alias("rows"),
            F.min("offset").alias("first_offset"),
            F.max("offset").alias("last_offset"),
            F.sum(F.col("json_parse_error").cast("int")).alias("json_errors"),
        )
        .orderBy("partition")
        .collect()
    )
    total = sum(row["rows"] for row in summaries)
    print(f"Batch {batch_id}: {total} messages read", flush=True)
    for row in summaries:
        print(
            f"  partition={row['partition']} rows={row['rows']} "
            f"offsets={row['first_offset']}..{row['last_offset']}",
            flush=True,
        )
    print(f"  JSON parse errors: {sum(row['json_errors'] for row in summaries)}",
          flush=True)
    if total:
        # Only show five examples. Every record is still parsed and counted.
        batch.select(
            "event_id", "order_id", "event_type", "quantity", "order_total_vnd",
            "event_time_utc", "json_parse_error",
        ).show(5, truncate=40)

spark = (
    SparkSession.builder
    .appName("RetailKafkaReader")
    .config("spark.sql.shuffle.partitions", "3")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

events = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "retail-order-events")
    .option("startingOffsets", "earliest")
    # startingOffsets applies only when there is no existing checkpoint.
    .option("maxOffsetsPerTrigger", 1000)
    .option("failOnDataLoss", "true")
    .load()
)

messages = events.selectExpr(
    "CAST(key AS STRING) AS order_key",
    "CAST(value AS STRING) AS event_json",
    "topic",
    "partition",
    "offset",
    "timestamp AS kafka_timestamp",
)

parsed_events = parse_events(messages)
parsed_events.printSchema()

query = (
    parsed_events.writeStream
    .foreachBatch(summarize_batch)
    .option("checkpointLocation", CHECKPOINT.as_uri())
    .trigger(processingTime="5 seconds")
    .start()
)

try:
    print(f"Checkpoint: {CHECKPOINT}", flush=True)
    print("Reading Kafka; press Ctrl+C to stop. No database writes.", flush=True)
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping Spark reader...", flush=True)
finally:
    query.stop()
    spark.stop()
