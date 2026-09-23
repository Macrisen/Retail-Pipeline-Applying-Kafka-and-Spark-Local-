"""Local Kafka -> PostgreSQL raw inbox. No changes to batch Bronze/Silver/Gold.

Default mode snapshots at most 1,000 retained messages and exits. --mode backfill
drains currently available data; --mode stream continues waiting for new data.
Never delete/recreate the topic during ingestion. Recreated topics get a new
Topic ID and a separate checkpoint. Database credentials use PG* environment vars.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def topic_identity(bootstrap, topic):
    result = subprocess.run(
        ["kafka-topics", "--bootstrap-server", bootstrap, "--describe", "--topic", topic],
        capture_output=True, text=True, timeout=30, check=True,
    )
    match = re.search(r"TopicId:\s*(\S+)\s+PartitionCount:\s*(\d+)", result.stdout)
    if not match:
        raise RuntimeError("Topic does not exist or Topic ID cannot be read. Create it explicitly.")
    return {"topic_id": match[1], "partitions": int(match[2])}


def write_rows(rows, source_cluster, topic_id, topic, connection_factory):
    """One transaction per bounded Spark batch; retries preserve existing rows."""
    count = 0
    with connection_factory() as conn:
        with conn.cursor() as cur:
            cur.execute("SET LOCAL statement_timeout = '120s'")
            cur.execute("SET LOCAL lock_timeout = '30s'")
            cur.execute("""CREATE TEMP TABLE kafka_stage (
                source_cluster text, source_topic_id text, topic text,
                partition_id integer, kafka_offset bigint,
                message_key bytea, raw_value bytea, kafka_timestamp_ms bigint
            ) ON COMMIT DROP""")
            with cur.copy("COPY kafka_stage FROM STDIN") as copy:
                for row in rows:
                    if row.topic != topic:
                        raise ValueError("Unexpected topic in batch")
                    copy.write_row((source_cluster, topic_id, row.topic,
                                    row.partition, row.offset,
                                    bytes(row.key) if row.key is not None else None,
                                    bytes(row.value) if row.value is not None else None,
                                    row.kafka_timestamp_ms))
                    count += 1
            # Serialize overlapping writers, then detect conflicting source coordinates.
            cur.execute("SELECT pg_advisory_xact_lock(74850, 300000)")
            cur.execute("""SELECT EXISTS (
                SELECT 1 FROM kafka_stage s JOIN bronze.kafka_sales_events d
                USING (source_cluster, source_topic_id, partition_id, kafka_offset)
                WHERE ROW(s.topic, s.message_key, s.raw_value, s.kafka_timestamp_ms)
                IS DISTINCT FROM ROW(d.topic, d.message_key, d.raw_value, d.kafka_timestamp_ms)
            )""")
            if cur.fetchone()[0]:
                raise ValueError("Same Kafka coordinates have different contents; batch rolled back")
            cur.execute("""INSERT INTO bronze.kafka_sales_events
                (source_cluster, source_topic_id, topic, partition_id, kafka_offset,
                 message_key, raw_value, kafka_timestamp_ms)
                SELECT * FROM kafka_stage
                ON CONFLICT (source_cluster, source_topic_id, partition_id, kafka_offset)
                DO NOTHING""")
            inserted = cur.rowcount
    # Connection context commits before returning; failures propagate to Spark.
    return count, inserted

def refresh_silver_gold():
    root = Path(__file__).resolve().parents[1]

    subprocess.run(
        [
            "psql",
            "-X",
            "-v", "ON_ERROR_STOP=1",
            "-f", str(root / "sql/12_kafka_bronze_to_silver.sql"),
            "-f", str(root / "sql/08_load_and_validate_gold.sql"),
        ],
        check=True,
    )

    print("Silver → Gold refreshed successfully.", flush=True)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="retail-order-events")
    parser.add_argument("--source-cluster", default="retail-local")
    parser.add_argument("--mode", choices=["preview", "backfill", "stream"], default="preview")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=1000,
                        help="Maximum Kafka messages per streaming batch (default: 1000)")
    args = parser.parse_args()
    if args.max_offsets_per_trigger <= 0:
        parser.error("--max-offsets-per-trigger must be greater than zero")
    if (
        args.topic != "retail-order-events"
        or args.source_cluster != "retail-local"
    ):
        parser.error("This script is only configured for retail-order-events on retail-local")
    for variable in ("PGHOST", "PGDATABASE", "PGUSER"):
        if not os.environ.get(variable):
            parser.error(f"Set {variable} before running (do not put passwords in source code)")
    import psycopg # pyright: ignore[reportMissingImports]
    from pyspark.sql import SparkSession, functions as F 

    def connect():
        # Native libpq environment settings, TLS by default; no credentials in Spark conf.
        return psycopg.connect(sslmode=os.environ.get("PGSSLMODE", "require"),
                               connect_timeout=15, prepare_threshold=None)

    identity = topic_identity(args.bootstrap_servers, args.topic)
    with connect() as conn:
        conn.execute("SELECT source_topic_id FROM bronze.kafka_sales_events LIMIT 0")
        database_identity = {name: conn.info.get_parameters().get(name, "")
                             for name in ("host", "port", "dbname", "user")}
    spark = (SparkSession.builder.appName("RetailKafkaToBronze")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    def project(frame):
        return frame.select("topic", "partition", "offset", "key", "value",
                            F.unix_millis("timestamp").alias("kafka_timestamp_ms"))

    def persist(batch, batch_id):
        if topic_identity(args.bootstrap_servers, args.topic) != identity:
            raise RuntimeError("Topic identity/partition count changed. Stop and review before retrying.")
        total, inserted = write_rows(batch.toLocalIterator(), args.source_cluster,
                                     identity["topic_id"], args.topic, connect)
        print(f"Batch {batch_id} committed: read={total}, inserted={inserted}, "
              f"already_present={total - inserted}", flush=True)
        if total > 0:
            refresh_silver_gold()
        

    source_options = {"kafka.bootstrap.servers": args.bootstrap_servers,
                      "subscribe": args.topic, "startingOffsets": "earliest",
                      "kafka.allow.auto.create.topics": "false", "failOnDataLoss": "true"}
    query = None
    try:
        print(f"Source: {args.topic}, Topic ID={identity['topic_id']}, "
              f"partitions={identity['partitions']}, mode={args.mode}", flush=True)
        if args.mode == "preview":
            # Bounded read; never limit inside a streaming batch and discard its remainder.
            frame = spark.read.format("kafka").options(**source_options).load()
            persist(project(frame).limit(1000), "preview")
        else:
            import hashlib
            binding = {"source_cluster": args.source_cluster, "topic": args.topic,
                       "topic_id": identity["topic_id"], "database": database_identity}
            checkpoint_key = hashlib.sha256(json.dumps(binding, sort_keys=True).encode()).hexdigest()[:24]
            checkpoint = (Path(__file__).resolve().parents[1] / ".local" / "checkpoints"
                          / f"kafka-bronze-{checkpoint_key}")
            checkpoint.mkdir(parents=True, exist_ok=True)
            binding_path = checkpoint / "source-binding.json"
            if binding_path.exists():
                if json.loads(binding_path.read_text()) != binding:
                    raise RuntimeError("Checkpoint belongs to a different source or database")
            else:
                binding_path.write_text(json.dumps(binding, indent=2) + "\n")
            events = (spark.readStream.format("kafka").options(**source_options)
                      .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger).load())
            writer = (project(events).writeStream.foreachBatch(persist)
                      .option("checkpointLocation", checkpoint.as_uri()))
            query = (writer.trigger(availableNow=True) if args.mode == "backfill"
                     else writer.trigger(processingTime="60 seconds")).start()
            print(f"Checkpoint: {checkpoint}", flush=True)
            query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping. Committed database rows will be retained.", flush=True)
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
