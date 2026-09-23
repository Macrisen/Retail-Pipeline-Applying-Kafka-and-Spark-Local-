-- Run after installing the updated 07 and completing the first load (08).
-- No Gold snapshots: transactional triggers reject changes during the rerun.
-- The loader still creates its own temporary staging tables.
BEGIN;
SET LOCAL TIME ZONE 'UTC';
SET LOCAL lock_timeout = '5s';
SELECT pg_advisory_xact_lock(74850,299400);
LOCK TABLE bronze.raw_sales, silver.sales_events IN SHARE MODE;
LOCK TABLE gold.dim_date, gold.dim_customer, gold.dim_product,
    gold.fact_sales_events, gold.fact_orders IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION pg_temp.retail_reject_gold_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF NEW IS NOT DISTINCT FROM OLD THEN
            RETURN NULL;
        END IF;
    END IF;
    RAISE EXCEPTION 'FAIL: rerun attempted % on %.%; Gold was not unchanged',
        TG_OP, TG_TABLE_SCHEMA, TG_TABLE_NAME;
END;
$$;

-- AFTER INSERT observes only rows actually inserted, not ON CONFLICT attempts.
-- UPDATE comparison includes every column and handles NULL.
CREATE TRIGGER retail_idempotency_rows
AFTER INSERT OR UPDATE OR DELETE ON gold.dim_date
FOR EACH ROW EXECUTE FUNCTION pg_temp.retail_reject_gold_change();
CREATE TRIGGER retail_idempotency_truncate
BEFORE TRUNCATE ON gold.dim_date
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.retail_reject_gold_change();

CREATE TRIGGER retail_idempotency_rows
AFTER INSERT OR UPDATE OR DELETE ON gold.dim_customer
FOR EACH ROW EXECUTE FUNCTION pg_temp.retail_reject_gold_change();
CREATE TRIGGER retail_idempotency_truncate
BEFORE TRUNCATE ON gold.dim_customer
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.retail_reject_gold_change();

CREATE TRIGGER retail_idempotency_rows
AFTER INSERT OR UPDATE OR DELETE ON gold.dim_product
FOR EACH ROW EXECUTE FUNCTION pg_temp.retail_reject_gold_change();
CREATE TRIGGER retail_idempotency_truncate
BEFORE TRUNCATE ON gold.dim_product
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.retail_reject_gold_change();

CREATE TRIGGER retail_idempotency_rows
AFTER INSERT OR UPDATE OR DELETE ON gold.fact_sales_events
FOR EACH ROW EXECUTE FUNCTION pg_temp.retail_reject_gold_change();
CREATE TRIGGER retail_idempotency_truncate
BEFORE TRUNCATE ON gold.fact_sales_events
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.retail_reject_gold_change();

CREATE TRIGGER retail_idempotency_rows
AFTER INSERT OR UPDATE OR DELETE ON gold.fact_orders
FOR EACH ROW EXECUTE FUNCTION pg_temp.retail_reject_gold_change();
CREATE TRIGGER retail_idempotency_truncate
BEFORE TRUNCATE ON gold.fact_orders
FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.retail_reject_gold_change();

CALL gold.load_from_silver();
SELECT 'PASS: all five Gold tables unchanged after rerun' AS idempotency_result;
-- Removes test triggers/function and rolls back the test load.
ROLLBACK;

