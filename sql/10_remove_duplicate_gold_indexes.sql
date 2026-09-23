-- Based on the supplied pg_indexes export: each index below duplicates
-- an existing non-unique B-tree index. Keep the original index names.
-- Run after the updated 06 script has ensured the original indexes exist.
BEGIN;
SET LOCAL lock_timeout = '5s';
DROP INDEX IF EXISTS gold.idx_fact_sales_events_order_id;
DROP INDEX IF EXISTS gold.idx_fact_sales_events_event_date_key;
DROP INDEX IF EXISTS gold.idx_fact_orders_created_date_key;
DROP INDEX IF EXISTS gold.idx_fact_orders_paid_date_key;
COMMIT;
