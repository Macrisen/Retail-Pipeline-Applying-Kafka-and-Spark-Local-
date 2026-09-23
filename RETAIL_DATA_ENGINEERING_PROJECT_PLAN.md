# Retail Streaming Data Engineering Project

## Current reading guide — 2026-09-18

The active path is CSV replay → Kafka → Spark → local PostgreSQL Bronze →
SQL Silver → SQL Gold → exported CSV snapshot → Power BI. See `sql/README.md`
for the installation order and `docs/TESTING_GUIDE_VI.md` for test commands.
The batch CSV/Supabase path below is retained as project history and as the
fixture for the SQL integration test. Dated status sections below record what
was known at that time; they are not one current checklist.

`05_silver_quality_checks.sql` installs the validator; call
`silver.prepare_gold_source()` after Silver contains data, or run 08, whose
loader calls it. The streaming Silver load rejects automatic corrections to an
existing `event_id` through validation. Each batch currently rechecks the full
Bronze/Silver/Gold source, and those writes span separate transactions.
Temporary PostgreSQL tests cover batch Gold and the Bronze sink. The separate
watermark exercise is not part of the main Kafka pipeline. Controlled
mid-batch Spark crash/recovery remains untested.

## 1. Project Goal

Build an end-to-end Data Engineering portfolio project using:

- PostgreSQL / Supabase
- Bronze / Silver / Gold architecture
- Dimensional modeling
- Kafka
- Spark Structured Streaming
- Data quality checks
- Idempotent loading
- Late-event handling
- Optional Power BI serving layer

The project uses a synthetic retail event dataset with approximately:

- **300,000 raw events**
- **74,850 orders**
- **600 duplicated events**
- Multiple events per order
- Late-arriving events

Typical order lifecycle:

```text
ORDER_CREATED
    ↓
PAYMENT_CONFIRMED
    ↓
ORDER_SHIPPED
    ↓
ORDER_DELIVERED
```

Some orders may instead follow:

```text
ORDER_CREATED
    ↓
PAYMENT_CONFIRMED
    ↓
ORDER_CANCELLED
    ↓
REFUND_ISSUED
```

---

# 2. Final Architecture

```text
Synthetic Retail Dataset
          │
          │ Historical Backfill
          ▼
   bronze.raw_sales
          │
          │ Clean / Validate
          │ Cast datatype
          │ Normalize NULL
          │ Deduplicate event_id
          ▼
 silver.sales_events
          │
          │ Business Modeling
          ▼
 ┌───────────────────────────────┐
 │           GOLD                │
 │                               │
 │ gold.dim_date                 │
 │ gold.dim_customer             │
 │ gold.dim_product              │
 │ gold.fact_sales_events        │
 │ gold.fact_orders              │
 └───────────────────────────────┘
          │
          ▼
 Analytics / Power BI

          +

Synthetic Event Replay
          │
          ▼
     Kafka Producer
          │
          ▼
         Kafka
          │
          ▼
 Spark Structured Streaming
          │
          ▼
 Bronze / Silver / Gold
```

---

# 3. Current Project Status

## Completed

- [x] Generate synthetic retail dataset
- [x] Upload raw dataset to Supabase/PostgreSQL
- [x] Create Bronze schema
- [x] Create `bronze.raw_sales`
- [x] Load raw events into Bronze
- [x] Create Silver schema
- [x] Create `silver.sales_events`
- [x] Load Bronze data into Silver
- [x] Convert / normalize data types
- [x] Convert blank promo code to NULL
- [x] Remove duplicate events by `event_id`
- [x] Split event timestamp into date and time
- [x] Split producer timestamp into date and time
- [x] Split loaded timestamp into date and time

## Current Position — 2026-09-13

```text
CSV replay → Kafka → Spark → Bronze local → SQL Silver → SQL Gold ✅
                                      ↓
             New-order and duplicate replay checks passed ✅
             Normal stop/restart checks passed ✅
                                      ↓
             Documentation / optional dashboard ⬅️ NEXT
```

The working streaming target is local PostgreSQL database `retail_streaming`.
Supabase retains the earlier batch project; the user reported it was full, so
streaming ingestion was implemented locally instead.

Spark writes raw Kafka messages to `bronze.kafka_sales_events`, then invokes
SQL Silver and Gold loading after each nonempty batch. The stream trigger is
60 seconds. Startup is manual; this is synthetic event replay, not live sales.

Sections 23 (end-to-end late-event test) and 25 (interruption during processing)
remain incomplete and have been deferred by the user. Normal restart checks
passed; they do not prove recovery from abrupt failure during a write.


---

# 4. Bronze Layer

Table:

```text
bronze.raw_sales
```

Grain:

> One row = one raw event

Bronze should preserve the source as closely as possible.

Expected characteristics:

- Raw rows are preserved
- Duplicate events remain
- No business aggregation
- No filtering by event type
- Used for replay, debugging, and rebuilding downstream layers

Expected row count:

```text
~300,000 rows
```

Example fields:

```text
event_id
order_id
event_type
event_time_utc
producer_time_utc
customer_id
product_id
category
quantity
unit_price_vnd
order_total_vnd
payment_method
city
device
traffic_source
promo_code
is_late_event
```

---

# 5. Silver Layer

Table:

```text
silver.sales_events
```

Grain:

> One row = one clean unique event

Silver contains technically cleaned and standardized data.

Transformations already implemented:

- Data type conversion
- String normalization
- Blank promo code → NULL
- Deduplication by `event_id`
- UTC date/time normalization
- Split event timestamp
- Split producer timestamp
- Split load timestamp

Current time-related columns:

```text
event_date_utc
event_time_utc

producer_date_utc
producer_time_utc

loaded_date_utc
loaded_time_utc
```

Date format currently used in Silver:

```text
YYYYDDMM
```

Expected row count after removing 600 duplicates:

```text
299,400 rows
```

---

# 6. Silver Data Quality Checks

Complete these checks before loading Gold.

## Row Count

```sql
SELECT COUNT(*) AS silver_rows
FROM silver.sales_events;
```

Expected:

```text
299400
```

## Duplicate Check

```sql
SELECT
    COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_rows
FROM silver.sales_events;
```

Expected:

```text
0
```

## Required Field Check

```sql
SELECT
    COUNT(*) FILTER (WHERE event_id IS NULL) AS null_event_id,
    COUNT(*) FILTER (WHERE order_id IS NULL) AS null_order_id,
    COUNT(*) FILTER (WHERE event_type IS NULL) AS null_event_type,
    COUNT(*) FILTER (WHERE event_date_utc IS NULL) AS null_event_date,
    COUNT(*) FILTER (WHERE event_time_utc IS NULL) AS null_event_time
FROM silver.sales_events;
```

## Numeric Validation

```sql
SELECT *
FROM silver.sales_events
WHERE quantity <= 0
   OR unit_price_vnd < 0
   OR order_total_vnd < 0;
```

Expected:

```text
0 invalid rows
```

## Event Type Validation

Allowed values should include:

```text
ORDER_CREATED
PAYMENT_CONFIRMED
ORDER_SHIPPED
ORDER_DELIVERED
ORDER_CANCELLED
REFUND_ISSUED
CANCEL_ACKNOWLEDGED
```

---

# 7. Gold Layer

The Gold layer is where business-oriented modeling begins.

Current Gold tables:

```text
gold.dim_date
gold.dim_customer
gold.dim_product
gold.fact_sales_events
gold.fact_orders
```

Recommended load order:

```text
1. gold.dim_date
2. gold.dim_customer
3. gold.dim_product
4. gold.fact_sales_events
5. gold.fact_orders
```

Dimensions must be loaded before facts because facts reference dimension keys.

---

# 8. Gold Dimension: dim_date

Purpose:

Create a reusable date dimension for reporting and warehouse joins.

Recommended key format:

```text
date_key = YYYYMMDD
```

Example conversion:

```text
Silver:
event_date_utc = 20261508   (YYYYDDMM)

Gold:
full_date = 2026-08-15
date_key  = 20260815         (YYYYMMDD)
```

Recommended attributes:

```text
date_key
full_date
date_label
day_of_month
month_number
quarter_number
year_number
day_of_week
is_weekend
```

---

# 9. Gold Dimension: dim_customer

Purpose:

Convert customer business identifiers into warehouse surrogate keys.

Example:

```text
customer_key | customer_id
-------------|-------------
1            | CUST-000001
2            | CUST-000002
```

Grain:

> One row = one customer

`customer_id` is the natural/business key.

`customer_key` is the warehouse surrogate key.

---

# 10. Gold Dimension: dim_product

Purpose:

Store product information used by fact tables.

For this synthetic dataset, use:

```text
product_id + category
```

as the unique business combination.

Reason:

The generated synthetic dataset may contain the same `product_id` in different categories.

Example:

```text
product_key | product_id | category
------------|------------|-------------
1           | SKU001     | Electronics
2           | SKU001     | Fashion
```

Grain:

> One row = one unique product/category combination

---

# 11. Gold Fact: fact_sales_events

Grain:

> One row = one unique clean event

This fact preserves the event-stream structure from Silver but replaces natural dimension attributes with warehouse keys.

Example fields:

```text
event_id
order_id
event_type
event_date_key
producer_date_key
customer_key
product_key
event_time_utc
producer_time_utc
quantity
unit_price_vnd
order_total_vnd
payment_method
city
device
traffic_source
promo_code
is_late_event
```

Useful for:

- Event volume analysis
- Event lifecycle analysis
- Cancellation analysis
- Delivery analysis
- Event latency analysis
- Producer delay analysis
- Streaming monitoring

Do **not** calculate revenue by summing all event rows directly.

---

# 12. Gold Fact: fact_orders

Grain:

> One row = one order

This table converts multiple lifecycle events into one business order record.

Silver example:

```text
ORD001 | ORDER_CREATED
ORD001 | PAYMENT_CONFIRMED
ORD001 | ORDER_SHIPPED
ORD001 | ORDER_DELIVERED
```

Gold order example:

```text
order_id           = ORD001
created_at          = ...
paid_at             = ...
shipped_at          = ...
delivered_at        = ...
latest_event_type   = ORDER_DELIVERED
order_total_vnd     = ...
customer_key        = ...
```

Recommended attributes:

```text
order_id
customer_key
created_date_key
paid_date_key
delivered_date_key
created_at_utc
paid_at_utc
shipped_at_utc
delivered_at_utc
latest_event_type
latest_event_at_utc
order_total_vnd
payment_method
city
device
traffic_source
promo_code
```

Main metrics from this table:

- Total orders
- Total revenue
- Average order value
- Cancellation rate
- Delivery rate

---

# 13. Revenue Business Rule

An order can appear multiple times in the event stream.

Example:

```text
ORDER_CREATED      500,000
PAYMENT_CONFIRMED  500,000
ORDER_SHIPPED      500,000
ORDER_DELIVERED    500,000
```

Incorrect:

```text
SUM = 2,000,000
```

Correct:

```text
Revenue = 500,000
```

Recommended revenue rule:

Use the order amount once per order through `gold.fact_orders`, or explicitly use `event_type = 'PAYMENT_CONFIRMED'`.

Revenue logic belongs in the Gold layer.

---

# 14. Optional Gold Aggregate Views

After fact tables are correct, optional views can be added:

```text
gold.daily_sales
gold.sales_by_category
gold.sales_by_city
gold.sales_by_payment_method
gold.order_status_summary
```

Useful KPIs:

```text
Total Revenue
Total Orders
Average Order Value
Cancellation Rate
Delivery Rate
Revenue by Category
Revenue by City
Payment Method Share
```

---

# 15. Gold Data Quality Checks

Validate:

- `event_id` unique in `fact_sales_events`
- `order_id` unique in `fact_orders`
- All foreign keys resolve to dimensions
- Revenue is not multiplied by event count
- `order_total_vnd >= 0`

Recommended reconciliation:

```text
COUNT(DISTINCT silver.order_id)
=
COUNT(gold.fact_orders)
```

if every order is intentionally preserved.

---

# 16. Idempotency Test

The pipeline should be safe to rerun.

Example:

```text
Before rerun:
Silver = 299,400

After rerun:
Silver = 299,400
```

Not:

```text
598,800
```

Use mechanisms such as:

```sql
ON CONFLICT (...) DO NOTHING
```

or UPSERT / MERGE where appropriate.

Repeat the same test for Gold dimensions and facts.

---

# 17. Recommended SQL Repository Structure

```text
sql/
├── 01_create_schemas.sql
├── 02_create_bronze.sql
├── 03_create_silver.sql
├── 04_bronze_to_silver.sql
├── 05_silver_quality_checks.sql
├── 06_create_gold.sql
├── 07_load_dim_date.sql
├── 08_load_dim_customer.sql
├── 09_load_dim_product.sql
├── 10_load_fact_sales_events.sql
├── 11_load_fact_orders.sql
├── 12_gold_quality_checks.sql
└── 13_idempotency_tests.sql
```

---

# 18. Batch Pipeline Completion Criteria

The batch portion is complete when:

- [x] Bronze contains raw events
- [x] Silver contains clean deduplicated events
- [x] Silver quality checks pass
- [x] `dim_date` loaded
- [x] `dim_customer` loaded
- [x] `dim_product` loaded
- [x] `fact_sales_events` loaded
- [x] `fact_orders` loaded
- [x] Gold quality checks pass
- [x] Synthetic payment/full-refund metrics validated (not accounting revenue recognition)
- [x] Silver → Gold rerun leaves all five Gold tables unchanged
- [x] Full CSV → Bronze → Silver → Gold rebuild in an empty local test database
- [x] Silver and Gold transformation reruns preserve existing data
- [ ] Idempotent Bronze file ingestion (CSV import currently runs once per rebuild)

At this point:

```text
CSV
 ↓
Bronze
 ↓
Silver
 ↓
Gold
```

is already a valid Data Engineering pipeline.

---

# 19. Phase 2: Kafka Streaming

Kafka should be added **after** the batch transformation logic is stable.

Architecture:

```text
Synthetic CSV
      ↓
Python Replay Producer
      ↓
Kafka Topic
retail-order-events
      ↓
Spark Structured Streaming
      ↓
Bronze
      ↓
Silver
      ↓
Gold
```

README wording should be transparent:

> Historical synthetic retail events are replayed through Kafka to simulate a real-time event stream.

---

# 20. Kafka Producer

Create a Python producer that:

1. Reads the CSV
2. Converts each row to JSON
3. Sends each row as one Kafka message
4. Uses `order_id` as the Kafka message key
5. Controls event rate

Example rates:

```text
50 events/sec
100 events/sec
500 events/sec
```

---

# 21. Kafka Topic Design

Topic:

```text
retail-order-events
```

Recommended message key:

```text
order_id
```

Reason:

Events from the same order should ideally remain in the same partition.

---

# 22. Spark Structured Streaming

Spark consumer should:

- Read Kafka messages
- Parse JSON
- Apply schema
- Validate fields
- Deduplicate `event_id`
- Handle event time
- Handle late-arriving events
- Use watermarking
- Use checkpointing
- Write downstream data

---

# 23. Late Event Handling

### Verification update (2026-09-15)

The Kafka → Bronze → Silver → Gold out-of-order test passed for
`ORD-LATE-5f5e472ad2cb`: delivery at `20:17:33 UTC` arrived before payment
at `19:17:33 UTC`. After payment arrived, Gold populated `paid_at_utc` while
retaining `ORDER_DELIVERED` as the latest event at `20:17:33 UTC`.

Run the separate controlled Spark file-stream watermark/window test:

```bash
SPARK_LOCAL_IP=127.0.0.1 spark-submit --master 'local[2]' streaming/test_watermark_windows.py
```

It uses 5-minute tumbling windows and a 10-minute watermark delay. It checks
that a late event within the delay is counted, an event for a finalized window
does not change its count, and finalized windows have the expected counts.
This is a local Spark test; watermark/window aggregation is not integrated
into the Kafka-to-Bronze or Gold pipeline. Earlier deferred-status notes below
describe the previous verification state.

Dataset includes intentionally late events.

Example:

```text
event_time    = 10:00
producer_time = 10:45
```

This can demonstrate:

- Event-time processing
- Producer delay
- Watermarks
- Late-arriving data
- Window aggregation

---

# 24. Streaming Deduplication

The dataset contains duplicate events.

Example:

```text
event_id = AAA
event_id = AAA
```

Desired Silver result:

```text
event_id = AAA
```

This demonstrates:

```text
at-least-once delivery
        ↓
deduplication
        ↓
idempotent downstream storage
```

---

# 25. Checkpoint and Recovery Test

Test fault tolerance:

1. Run streaming pipeline
2. Process first ~50,000 events
3. Stop consumer
4. Restart consumer
5. Continue processing
6. Verify no incorrect duplicate growth
7. Verify checkpoint recovery works

This demonstrates:

- Checkpointing
- Fault tolerance
- Restart recovery
- Reliable streaming ingestion

---

# 26. Optional Power BI Layer

Power BI should connect to Gold, not Bronze.

Correct:

```text
Power BI
   ↑
 Gold
```

Suggested dashboard metrics:

```text
Total Revenue
Total Orders
Average Order Value
Cancellation Rate
Delivery Rate
Revenue Trend
Revenue by Category
Revenue by City
Payment Method Share
```

---

# 27. Recommended Repository Structure

```text
retail-streaming-data-pipeline/
│
├── README.md
│
├── data/
│   └── sample/
│
├── sql/
│   ├── 01_create_schemas.sql
│   ├── 02_create_bronze.sql
│   ├── 03_create_silver.sql
│   ├── 04_bronze_to_silver.sql
│   ├── 05_silver_quality_checks.sql
│   ├── 06_create_gold.sql
│   ├── 07_load_dim_date.sql
│   ├── 08_load_dim_customer.sql
│   ├── 09_load_dim_product.sql
│   ├── 10_load_fact_sales_events.sql
│   ├── 11_load_fact_orders.sql
│   ├── 12_gold_quality_checks.sql
│   └── 13_idempotency_tests.sql
│
├── producer/
│   └── kafka_producer.py
│
├── streaming/
│   └── spark_streaming.py
│
├── config/
│   └── config.example.env
│
├── docs/
│   ├── architecture.md
│   └── data_model.md
│
└── tests/
    └── data_quality/
```

---

# 28. Final README Story

The final README should explain:

## Problem

Build a reliable event-driven retail data pipeline for order lifecycle analytics.

## Dataset

Synthetic retail event data with duplicates and late-arriving events.

## Architecture

Bronze / Silver / Gold + Kafka + Spark.

## Data Quality

Explain:

- Duplicate handling
- NULL handling
- Datatype normalization
- Validation
- Idempotency

## Data Modeling

Explain:

- Grain of each table
- Natural keys
- Surrogate keys
- Fact tables
- Dimension tables

## Streaming

Explain:

- Kafka topic
- Partition key
- Spark Structured Streaming
- Watermarks
- Checkpointing
- Late events

## Results

Document:

- Raw event count
- Clean event count
- Unique order count
- Duplicate count removed
- Streaming throughput
- Recovery test results

---

# 29. CV Description Draft

Possible project title:

**Real-Time Retail Data Pipeline | Kafka, Spark, PostgreSQL, Supabase**

Possible CV bullets:

- Built an event-driven retail data pipeline processing 300K order lifecycle events using PostgreSQL, Kafka, and Spark Structured Streaming.
- Implemented Bronze/Silver/Gold architecture with event-level deduplication, data quality validation, and dimensional modeling for downstream analytics.
- Designed idempotent loading and late-event handling to support safe reprocessing and reliable streaming ingestion.
- Modeled event-level and order-level fact tables with customer, product, and date dimensions for business reporting.

Only keep bullets that are actually implemented and tested.

---

# 30. Master Checklist

## Source and Bronze

- [x] Generate 300K synthetic retail event dataset
- [x] Upload data to Supabase
- [x] Create Bronze schema
- [x] Create `bronze.raw_sales`
- [x] Load raw data

## Silver

- [x] Create Silver schema
- [x] Create `silver.sales_events`
- [x] Convert datatypes
- [x] Normalize NULL / promo code
- [x] Deduplicate by `event_id`
- [x] Split event date/time
- [x] Split producer date/time
- [x] Split loaded date/time
- [x] Run Silver data-quality checks
- [x] Document Bronze → Silver row reconciliation

## Gold

- [x] Review Gold schemas against current Silver schema
- [x] Load `gold.dim_date`
- [x] Load `gold.dim_customer`
- [x] Load `gold.dim_product`
- [x] Load `gold.fact_sales_events`
- [x] Load `gold.fact_orders`
- [x] Implement synthetic gross-paid/full-refund/net-paid rule
- [x] Run Gold data-quality checks
- [x] Validate fact/dimension foreign keys
- [x] Validate unique order count
- [x] Test Gold idempotency

## Batch Pipeline

- [x] Save Bronze/Silver scripts 01–04 alongside existing Gold scripts
- [x] Save CSV import and rebuild instructions in `sql/README.md`
- [x] Run full pipeline from scratch in disposable local PostgreSQL
- [x] Rerun Silver and Gold transformations with Bronze unchanged
- [x] Confirm transformation reruns do not change data or increase row counts
- [ ] Make repeated Bronze CSV ingestion idempotent (not covered by this test)
- [ ] Document batch architecture

## Kafka

- [x] Install/configure local Kafka broker
- [x] Create `retail-order-events` topic
- [x] Build Python replay producer
- [x] Use `order_id` as Kafka message key
- [x] Configure event replay rate
- [x] Validate produced messages and replay the original 300,000 events
- [x] Install Kafbat UI and document start/stop commands
- [x] Diagnose topic recreation with one partition from broker logs
- [x] Test a separate three-partition topic `retail-order-events-v2`
- [ ] Prevent accidental topic auto-creation / enforce producer partition expectations

Main topic remains `retail-order-events` with one partition; v2 is test-only.
Three partitions were initially created, but deletion followed by broker
successful auto-creation used `num.partitions=1`. This was not a Spark thread limit.

## Spark Streaming

- [x] Connect Spark Structured Streaming to Kafka
- [x] Parse JSON into the 17 source fields and report parse errors
- [x] Preserve raw key/value bytes and Kafka coordinates in local Bronze
- [x] Apply field/type checks through downstream SQL (not a Spark validation pipeline)
- [x] Deduplicate `event_id` in SQL Silver; duplicate replay leaves Silver/Gold counts unchanged
- [x] Reuse Gold SQL event-time ordering logic
- [ ] Implement a Spark watermark
- [x] Verify late-event behavior through Kafka → Spark → Gold (section 23)
- [x] Add persistent source/destination-bound checkpointing
- [x] Write streaming output to local `bronze.kafka_sales_events`
- [x] Automatically invoke SQL Silver → Gold after a nonempty Bronze batch
- [x] Validate new-order propagation through the complete local pipeline
- [x] Verify normal restart with unchanged row counts and no new messages
- [x] Verify an order sent while Spark was stopped reaches Gold after restart
- [x] Process the 100-event RECOVERY-01 fixture through Gold
- [ ] Interrupt an active batch and verify complete recovery without duplicates (section 25, deferred)
- [ ] Optimize downstream SQL to process only new/affected data

The 100-event fixture completed normally. Reading intermediate database counts
while it ran demonstrated separate tier commits, not a failure/recovery test.
Bronze, Silver and Gold commit separately; full-source SQL refresh is still used.
The observed 28-second batch exceeded the former 5-second trigger. The trigger
was changed to 60 seconds; this is scheduling, not a SQL performance optimization.

## Serving

- [x] Export all six Gold tables/views and load them into Power BI
- [x] Build the three-page KPI dashboard and semantic relationships
- [x] Validate key dashboard measures against Gold exports

Section 26 was completed from local PostgreSQL through six CSV exports. The
Power BI model filters 103 pipeline-test orders from business reporting and uses
the 74,850 original orders. The report covers sales overview, product/customer
analysis, and order/streaming operations. See
`docs/POWER_BI_DAX_MEASURES.md` for relationships, DAX and visual definitions.

## Documentation

- [ ] Architecture diagram
- [ ] Data model diagram
- [ ] Explain table grains
- [ ] Explain business rules
- [ ] Explain duplicate handling
- [ ] Explain idempotency
- [x] Record current Kafka partition setup and topic-recreation incident in this plan
- [ ] Explain late-event handling
- [x] Record normal-restart evidence and remaining recovery-test limits in this plan
- [ ] Finalize README
- [ ] Add final CV bullets

---

# 31. Immediate Next Task

## Confirmed execution — 2026-09-11

The user confirmed successful execution of Silver validation, Gold setup/loading,
and the updated Gold idempotency test on Supabase. File 09 completed through psql
using the Session pooler after SQL Editor timed out. The reported final result was:

```text
PASS: all five Gold tables unchanged after rerun
(1 row)
ROLLBACK
```

This Supabase result validates the Silver → Gold rerun. The separate local
rebuild and transformation rerun are now verified below; repeated CSV ingestion
was not tested as an idempotent operation.
The rollback removed the test triggers and preserved the original Gold load.

Reconciliation: Bronze has 300,000 rows; Silver has 299,400 unique events after
removing 600 duplicate rows. The validator compares all 17 business fields after
normalizing promo codes and rebuilding UTC timestamps.

Gold counts validated locally on the full CSV and consistent with the supplied
server statistics: 37 dates, 26,054 customers, 34,996 product/category combinations,
299,400 event facts, and 74,850 order facts. Server statistics are estimates;
loader reconciliation and successful validation provide the stronger checks.

File 08 returned these payment metrics on Supabase:

- Gross paid: 266,858,794,000 VND
- Full refunds: 6,317,929,000 VND
- Net paid: 260,540,865,000 VND

These are synthetic payment-flow metrics, not accounting revenue recognition.
Refunds are assumed to cover the full order value.

Four duplicate Gold indexes were removed (the user confirmed zero remaining).
The last reported database size was 457 MB. File 07 now skips unchanged row
updates. File 09 uses transactional change-detection triggers instead of two
JSON snapshots; the loader still needs temporary staging space.

Existing scripts:

1. `sql/05_silver_quality_checks.sql`
2. `sql/06_review_gold_schema.sql`
3. `sql/07_install_gold_loader.sql`
4. `sql/08_load_and_validate_gold.sql`
5. `sql/09_test_gold_idempotency.sql`
6. `sql/10_remove_duplicate_gold_indexes.sql` (one-time cleanup)

See `sql/README.md` for execution details. Local integration tests also passed
invalid-input, NULL-update, late-arrival, unchanged-update and trigger-cleanup cases.

## Local batch rebuild verified — 2026-09-11

Executed `python3 tests/test_gold_pipeline.py` against the full `raw_sales.csv`
in a disposable PostgreSQL cluster. The command exited with code 0 and reported
`ALL TESTS PASSED`. The temporary cluster was stopped and removed. This run did
not connect to or change Supabase.

Completed work:

- Saved schema creation, Bronze DDL, final Silver DDL and clean/load logic in
  `sql/01_create_schemas.sql` through `sql/04_bronze_to_silver.sql`.
- Preserved original supplied DDL in `docs/history/supabase_original_ddl.txt`.
- Saved one-time CSV import and rebuild instructions in `sql/README.md`.
- Created an empty database, imported CSV once, and loaded Silver and Gold.
- Verified Silver rerun preserves all columns, including load date/time.
- Verified Gold rerun preserves all five tables and performs no unchanged-row updates.
- Passed source reconciliation, payment/refund checks, invalid-input rejection,
  changed-row and NULL updates, late-arrival ordering, trigger cleanup and repeat installation.

| Table | Verified local row count |
|---|---:|
| bronze.raw_sales | 300,000 |
| silver.sales_events | 299,400 |
| gold.dim_date | 37 |
| gold.dim_customer | 26,054 |
| gold.dim_product | 34,996 |
| gold.fact_sales_events | 299,400 |
| gold.fact_orders | 74,850 |

Local payment totals matched the previously reported values:
266,858,794,000 VND gross paid, 6,317,929,000 VND refunded and
260,540,865,000 VND net paid.

### Verification boundary

The verified sequence is CSV import once → Bronze → Silver → Gold, followed by
transformation reruns with Bronze unchanged. Repeating the raw CSV import would
append another copy to Bronze; automatic ingestion deduplication requires a
separate file/batch tracking policy. No full rebuild was performed on Supabase.

### Historical next task

Kafka setup and CSV replay were next after the 2026-09-11 batch rebuild. They
have since been implemented; the current state is recorded below.

## Local streaming progress verified — 2026-09-13

Evidence: user-provided producer/Spark logs, PostgreSQL counts and Gold test
output. No new database load was run while updating this checklist.

Current execution flow:

```text
raw_sales.csv
  → producer.py (manual launch)
  → Kafka: retail-order-events (one partition)
  → streaming/kafka_to_bronze.py --mode stream
  → bronze.kafka_sales_events (raw bytes + Topic ID/partition/offset)
  → sql/12_kafka_bronze_to_silver.sql
  → silver.sales_events
  → sql/08_load_and_validate_gold.sql
  → gold dimensions and facts
```

Compatibility view `bronze.raw_sales`, defined by
`sql/13_kafka_bronze_compat_view.sql`, reads the main-topic Kafka inbox so existing
Bronze/Silver reconciliation and Gold loaders can be reused. In this local
database it is a view; the earlier batch setup used a physical table with that name.

### Confirmed row counts

| Checkpoint in the work | Bronze messages | Silver events | Gold events | Gold orders |
|---|---:|---:|---:|---:|
| Original dataset loaded | 300,000 | 299,400 | 299,400 | 74,850 |
| Replay of 10 existing events | 300,010 | 299,400 | 299,400 | 74,850 |
| Two individually created test orders | 300,012 | 299,402 | 299,402 | 74,852 |
| After 100 RECOVERY-01 test orders | 300,112 | 299,502 | 299,502 | 74,952 |

These are local counts. The latest database includes 102 extra test orders and
10 replayed messages; original CSV baseline counts should not be mistaken for
current totals. Test orders were ORDER_CREATED events, not new paid orders.

During the 100-order test, Bronze and Silver first increased while Gold retained
its previous counts. Gold then caught up to 299,502 events and 74,952 orders.
The user confirmed `Silver → Gold refreshed successfully.` in the automated flow.

The local Gold rerun test also returned:

```text
PASS: all five Gold tables unchanged after rerun
ROLLBACK
```

ROLLBACK ends the transactional test and removes its temporary test changes;
it is not a failed load or a deletion of the previously committed Gold data.

### Recovery and idempotency boundaries

- Bronze uses source cluster + Topic ID + partition + offset as its unique key.
  Source duplicates with different offsets are retained for Silver deduplication.
- The local sink integration test passed first-load, replay, raw bytes/NULL,
  rollback on conflict/source interruption and recreated-topic cases in a
  disposable PostgreSQL instance. This is separate from end-to-end Spark recovery.
- SQL Silver uses ON CONFLICT(event_id) DO NOTHING; corrections to an existing
  event_id are not automatically applied.
- A new order sent while Spark was stopped was present in Gold after restart.
- A subsequent restart without new events preserved the observed four row counts.
  Counts alone do not prove that no records were reread: the sink also prevents duplicates.
- No controlled mid-batch interruption or abrupt-crash test was completed. The
  user chose to defer it after the 100-event normal run succeeded.
- Gold event-time behavior passed earlier batch integration tests, but the
  end-to-end streaming late-event scenario is still deferred. No watermark exists.
- Spark now calls Silver/Gold after each nonempty batch, including replayed
  batches with zero new Bronze inserts. Database commits remain separate.
- Pipeline launch is manual. Automatic process restart, alerting and production
  orchestration have not been implemented.

### Current next tasks

1. Consolidate the README, architecture and local runbook around the current
   local pipeline; older Supabase-oriented instructions need context.
2. Keep the completed Power BI dashboard aligned with future Gold schema/data
   changes; section 26 currently uses a CSV snapshot rather than live refresh.
3. Section 23 late-event and the separate watermark/window tests are complete.
   Section 25 mid-processing recovery was skipped by user choice.
4. Optimize full-source Silver/Gold refresh and explicitly manage topic creation.

Operational references: `docs/KAFKA_START_STOP.md`, `docs/KAFKA_TO_BRONZE.md`.
The latter was originally written for Supabase; current connection target is
local `retail_streaming`, and Silver/Gold invocation has since been added to the writer.
