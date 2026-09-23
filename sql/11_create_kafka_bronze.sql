-- Run once in Supabase SQL Editor as the schema owner.
-- Separate raw streaming inbox; existing batch tables are unchanged.
BEGIN;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE TABLE IF NOT EXISTS bronze.kafka_sales_events (
    source_cluster TEXT NOT NULL,
    source_topic_id TEXT NOT NULL,
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    kafka_offset BIGINT NOT NULL CHECK (kafka_offset >= 0),
    message_key BYTEA,
    raw_value BYTEA,
    kafka_timestamp_ms BIGINT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (source_cluster, source_topic_id, partition_id, kafka_offset)
);
-- Preserve exact bytes, including malformed JSON, invalid UTF-8 and tombstones.
COMMENT ON COLUMN bronze.kafka_sales_events.raw_value IS
    'Original Kafka value bytes; NULL is a Kafka tombstone. Decode/parse in Silver.';
-- Access via the database owner/backend, not the public Supabase API.
ALTER TABLE bronze.kafka_sales_events ENABLE ROW LEVEL SECURITY;
COMMIT;
