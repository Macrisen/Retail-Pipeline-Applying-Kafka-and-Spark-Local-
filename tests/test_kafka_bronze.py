"""Disposable local PostgreSQL test; no Kafka or Supabase connections."""
import importlib.util
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace

import psycopg

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("sink", ROOT / "streaming/kafka_to_bronze.py")
sink = importlib.util.module_from_spec(spec)
spec.loader.exec_module(sink)
PG = Path('/opt/homebrew/opt/postgresql@17/bin')

with tempfile.TemporaryDirectory(prefix="bronze-sink-test-", dir="/private/tmp") as tmp:
    def run(args):
        subprocess.run([str(x) for x in args], check=True, capture_output=True, text=True)

    def connect():
        return psycopg.connect(host=tmp, port=55443, dbname="postgres", sslmode="disable")

    def record(offset, value=b'{"event_id":"duplicate"}', partition=0):
        return SimpleNamespace(topic="retail-order-events", partition=partition,
                               offset=offset, key=b"ORD1", value=value,
                               kafka_timestamp_ms=1780000000123)

    def write(rows, topic_id="original-topic"):
        return sink.write_rows(rows, "retail-local", topic_id, "retail-order-events", connect)

    def snapshot():
        with connect() as conn:
            return conn.execute("SELECT *, xmin::text FROM bronze.kafka_sales_events "
                                "ORDER BY source_topic_id, partition_id, kafka_offset").fetchall()

    run([PG/'initdb', '-D', tmp+'/data', '--locale=C', '--encoding=UTF8',
         '--auth-local=trust', '--auth-host=reject'])
    started = False
    try:
        run([PG/'pg_ctl', '-D', tmp+'/data', '-l', tmp+'/log', '-o',
             f"-k {tmp} -p 55443 -c listen_addresses=''", '-w', 'start'])
        started = True
        with connect() as conn:
            conn.execute((ROOT / 'sql/11_create_kafka_bronze.sql').read_text())
        rows = [record(0), record(1), record(2, b'not JSON\xff\x00'), record(3, None),
                record(0, partition=1)]
        assert write(rows) == (5, 5)
        before = snapshot()
        assert write(rows) == (5, 0)
        assert snapshot() == before  # includes ingested_at and tuple version
        try:
            write([record(4), record(0, b'changed')])
        except ValueError:
            pass
        else:
            raise AssertionError('Conflicting payload was accepted')
        assert snapshot() == before
        def interrupted():
            yield record(4)
            raise RuntimeError('simulated source read failure')
        try:
            write(interrupted())
        except RuntimeError:
            pass
        else:
            raise AssertionError('Expected source failure')
        assert snapshot() == before
        assert write([record(0)], topic_id="recreated-topic") == (1, 1)
        with connect() as conn:
            raw = conn.execute("SELECT raw_value FROM bronze.kafka_sales_events "
                               "WHERE kafka_offset=2").fetchone()[0]
            assert raw == b'not JSON\xff\x00'
        assert write([]) == (0, 0)
        print('PASS: first load, rerun unchanged, source duplicates retained, raw bytes/null, '
              'conflict rollback, interrupted COPY rollback, recreated topic, empty batch.')
    finally:
        if started:
            run([PG/'pg_ctl', '-D', tmp+'/data', '-m', 'fast', '-w', 'stop'])
