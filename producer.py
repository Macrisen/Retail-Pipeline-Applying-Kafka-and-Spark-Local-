"""Replay raw CSV rows into Kafka. Each invocation starts at the first row.

Keep CSV values as strings (including timestamps, numbers and blank fields).
Type conversion and event_id deduplication belong downstream in Silver.
Duplicates in the source are intentionally replayed.
"""

import argparse
import csv
import json
import math
from pathlib import Path
import sys
import time

from kafka import KafkaProducer
from kafka.errors import KafkaError 


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path,
                        default=Path(__file__).resolve().with_name("raw_sales.csv"))
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="retail-order-events")
    parser.add_argument("--rate", type=float, default=10,
                        help="Maximum events per second (default: 10).")
    parser.add_argument("--limit", type=int, default=100,
                        help="Number of rows to send; 0 sends the entire CSV (default: 100).")
    args = parser.parse_args()
    if not math.isfinite(args.rate) or args.rate <= 0:
        parser.error("--rate must be a finite positive number")
    if args.limit < 0:
        parser.error("--limit must be >= 0")
    return args


def main():
    args = parse_args()
    producer = None
    confirmed = 0
    try:
        with args.csv.open(newline="", encoding="utf-8-sig") as source:
            reader = csv.DictReader(source)
            headers = reader.fieldnames
            if (not headers or not {"event_id", "order_id"}.issubset(headers)
                    or len(headers) != len(set(headers))):
                raise ValueError("CSV must have unique headers including event_id and order_id")

            producer = KafkaProducer(
                bootstrap_servers=args.bootstrap_servers,
                client_id="retail-csv-replay",
                key_serializer=lambda key: key.encode("utf-8"),
                value_serializer=lambda row: json.dumps(
                    row, ensure_ascii=False, allow_nan=False
                ).encode("utf-8"),
                acks="all",
                enable_idempotence=True,
                max_block_ms=10000,
                request_timeout_ms=10000,
            )
            if not producer.partitions_for(args.topic):
                raise ValueError(f"No partitions available for topic {args.topic}")

            print(f"Sending {args.csv.name} → {args.topic}; "
                  f"rate <= {args.rate}/s, limit={args.limit or 'all'}", flush=True)
            next_send = time.monotonic()
            for row in reader:
                if args.limit and confirmed >= args.limit:
                    break
                if (None in row or any(value is None for value in row.values())
                        or not row["event_id"].strip() or not row["order_id"].strip()):
                    raise ValueError(f"Malformed CSV record at line {reader.line_num}")

                time.sleep(max(0, next_send - time.monotonic()))
                next_send = time.monotonic() + 1 / args.rate
                # Await broker acknowledgement so errors are not silently ignored.
                metadata = producer.send(
                    args.topic, key=row["order_id"], value=row
                ).get(timeout=30)
                confirmed += 1
                if confirmed <= 5 or confirmed % 100 == 0:
                    print(f"Confirmed {confirmed}: event_id={row['event_id']} "
                          f"partition={metadata.partition} offset={metadata.offset}",
                          flush=True)

            print(f"Complete: {confirmed} events acknowledged by Kafka.", flush=True)
            return 0
    except KeyboardInterrupt:
        print(f"\nStopped. {confirmed} events acknowledged.", file=sys.stderr)
        print("An in-flight event may still arrive. Restarting replays from row 1.",
              file=sys.stderr)
        return 130
    except (OSError, ValueError, csv.Error, KafkaError) as error:
        print(f"Failed after {confirmed} acknowledged events: {error}", file=sys.stderr)
        print("An in-flight event may still arrive. Restarting replays from row 1.",
              file=sys.stderr)
        return 1
    finally:
        if producer is not None:
            producer.close(timeout=10)


if __name__ == "__main__":
    sys.exit(main())
