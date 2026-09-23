"""Controlled file-stream test of Spark watermarking; no Kafka/database writes."""

import json
from pathlib import Path
import tempfile

from pyspark.sql import SparkSession, functions as F


def main():
    root = Path(__file__).resolve().parents[1] / ".local"
    root.mkdir(exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix="watermark-test-", dir=root))
    incoming = run / "input"
    incoming.mkdir()
    spark = (SparkSession.builder.appName("RetailWatermarkTest")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    query = None
    try:
        events = (spark.readStream.schema("event_id STRING, event_time_utc STRING")
                  .json(str(incoming))
                  .withColumn("event_time", F.to_timestamp("event_time_utc")))
        counts = (events.withWatermark("event_time", "10 minutes")
                  .groupBy(F.window("event_time", "5 minutes")).count())
        query = (counts.writeStream.format("memory")
                 .queryName("retail_watermark_test").outputMode("append")
                 .option("checkpointLocation", str(run / "checkpoint"))
                 .trigger(processingTime="1 second").start())

        def send(number, time, description):
            # Publish a complete file atomically; process separately to control arrival order.
            pending = run / "pending.json"
            pending.write_text(json.dumps({
                "event_id": str(number),
                "event_time_utc": f"2026-09-14T{time}:00Z",
            }) + "\n", encoding="utf-8")
            pending.rename(incoming / f"{number}.json")
            query.processAllAvailable()
            print(description, flush=True)
            print("Watermark:", query.lastProgress.get("eventTime", {}).get("watermark"),
                  flush=True)

        send(1, "10:01", "1. Event 10:01: window 10:00–10:05")
        send(2, "10:20", "2. Event 10:20: advance watermark to 10:10")
        send(3, "10:14", "3. Late event 10:14: accepted within delay")
        send(4, "10:02", "4. Late event 10:02: window already closed, dropped")
        send(5, "10:40", "5. Event 10:40: finalize earlier windows")
        result = spark.sql("""SELECT window.start AS start, window.end AS end, count
                              FROM retail_watermark_test ORDER BY start""")
        result.show(truncate=False)
        actual = {row["time"]: row["count"] for row in
                  result.select(F.date_format("start", "HH:mm").alias("time"),
                                "count").collect()}
        expected = {"10:00": 1, "10:10": 1, "10:20": 1}
        if actual != expected:
            raise AssertionError(f"Expected {expected}, got {actual}")
        print("PASS: window counts correct; late event accepted; closed-window event dropped.",
              flush=True)
        print("Test files:", run, flush=True)
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
