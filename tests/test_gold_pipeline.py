"""Integration test against a disposable PostgreSQL cluster in /private/tmp.
No network listener, no Supabase connection, always stops and removes test data.
Run: python3 tests/test_gold_pipeline.py
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
PG = Path('/opt/homebrew/opt/postgresql@17/bin')
if not PG.exists():
    PG = Path(subprocess.check_output(['pg_config', '--bindir'], text=True).strip())

with tempfile.TemporaryDirectory(prefix='retail-test-', dir='/private/tmp') as tmp:
    cluster = Path(tmp)
    env = dict(os.environ, PGHOST=tmp, PGPORT='55439', PGDATABASE='postgres', PGUSER=os.environ.get('USER','macrisen'))
    started = False
    def run(args, **kw):
        return subprocess.run([str(a) for a in args], env=env, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kw)
    def query(text, expect_error=None):
        # Send SQL as one batch after a query, matching the editor failure mode.
        if '\\copy' in text:
            result = run([PG/'psql', '-X', '-v', 'ON_ERROR_STOP=1'], input=text)
        else:
            result = run([PG/'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-c', 'SELECT 1;\n' + text])
        if expect_error:
            assert result.returncode and expect_error in result.stdout, result.stdout
        elif result.returncode:
            raise AssertionError(result.stdout)
        return result.stdout
    try:
        for args in [
            [PG/'initdb','-D',cluster/'data','--encoding=UTF8','--locale=C','--auth-local=trust','--auth-host=reject'],
            [PG/'pg_ctl','-D',cluster/'data','-l',cluster/'server.log','-o',f"-k {tmp} -p 55439 -c listen_addresses=''",'-w','start']
        ]:
            result=run(args)
            if result.returncode: raise RuntimeError(result.stdout)
        started=True
        raw=str(ROOT/'raw_sales.csv').replace("'", "''")
        for name in ['01_create_schemas.sql','02_create_bronze.sql','03_create_silver.sql']:
            query((ROOT/'sql'/name).read_text())
        query(f"\\copy bronze.raw_sales FROM '{raw}' WITH (FORMAT csv, HEADER true)\n")
        bronze_to_silver=(ROOT/'sql'/'04_bronze_to_silver.sql').read_text()
        query(bronze_to_silver)
        # Verify a second Silver load preserves all values, including load metadata.
        query("CREATE TABLE silver_before_test AS TABLE silver.sales_events;")
        query(bronze_to_silver)
        query("""DO $$ BEGIN
 IF EXISTS (
  (SELECT * FROM silver.sales_events EXCEPT ALL SELECT * FROM silver_before_test)
  UNION ALL
  (SELECT * FROM silver_before_test EXCEPT ALL SELECT * FROM silver.sales_events)
 ) THEN RAISE EXCEPTION 'Silver rerun changed data or load metadata'; END IF;
END $$;
DROP TABLE silver_before_test;""")
        print('01-04 fresh setup and Silver rerun PASS',flush=True)
        for name in ['05_silver_quality_checks.sql','06_review_gold_schema.sql',
                     '07_install_gold_loader.sql','08_load_and_validate_gold.sql',
                     '09_test_gold_idempotency.sql']:
            output=query((ROOT/'sql'/name).read_text())
            print(name, 'PASS', flush=True)
            if name.startswith('08'): print(output, flush=True)
        query('BEGIN; SET LOCAL TIME ZONE \'UTC\'; CALL silver.prepare_gold_source(); COMMIT;')
        print('explicit Silver validation PASS', flush=True)
        guard_sql = (ROOT/'sql'/'09_test_gold_idempotency.sql').read_text()
        for mutation in [
            "INSERT INTO gold.dim_customer(customer_id) VALUES ('TEST-NEW');",
            "UPDATE gold.fact_orders SET promo_code='TEST' WHERE order_id='ORD-00000002';",
            "DELETE FROM gold.fact_orders WHERE order_id='ORD-00000002';",
            "TRUNCATE gold.fact_orders;",
            "UPDATE gold.dim_product SET category='TEST' WHERE product_key=1;",
        ]:
            query(guard_sql.replace('CALL gold.load_from_silver();', mutation), 'FAIL: rerun attempted')
        query("""DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_trigger WHERE tgname IN
 ('retail_idempotency_rows','retail_idempotency_truncate'))
 THEN RAISE EXCEPTION 'Test triggers were not removed'; END IF;
END $$;""")
        print('idempotency guard rejects changes and cleans up triggers PASS', flush=True)
        # Fail on any actual UPDATE during an unchanged reload, including NULL fields.
        query("""BEGIN;
CREATE FUNCTION pg_temp.reject_gold_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Unchanged reload attempted an UPDATE'; END $$;
CREATE TRIGGER reject_reload_update BEFORE UPDATE ON gold.dim_date
 FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_gold_update();
CREATE TRIGGER reject_reload_update BEFORE UPDATE ON gold.fact_sales_events
 FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_gold_update();
CREATE TRIGGER reject_reload_update BEFORE UPDATE ON gold.fact_orders
 FOR EACH ROW EXECUTE FUNCTION pg_temp.reject_gold_update();
CALL gold.load_from_silver();
ROLLBACK;""")
        print('unchanged reload performs zero row UPDATEs PASS', flush=True)
        # Changed values, including NULL transitions, still get repaired from source.
        query("""BEGIN;
UPDATE gold.dim_date SET date_label='wrong' WHERE date_key=20260829;
UPDATE gold.fact_sales_events SET promo_code=NULL
 WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f';
UPDATE gold.fact_orders SET promo_code='wrong' WHERE order_id='ORD-00000002';
CALL gold.load_from_silver();
DO $$ BEGIN
 IF (SELECT date_label FROM gold.dim_date WHERE date_key=20260829) IS DISTINCT FROM '20262908'
 OR (SELECT promo_code FROM gold.fact_sales_events WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f') IS DISTINCT FROM 'FREESHIP'
 OR (SELECT promo_code FROM gold.fact_orders WHERE order_id='ORD-00000002') IS NOT NULL
 THEN RAISE EXCEPTION 'Changed row or NULL transition was not updated'; END IF;
END $$;
ROLLBACK;""")
        print('changed rows and NULL transitions update correctly PASS', flush=True)
        # Independent source-derived assertions, not just loader staging comparisons.
        print(query('''DO $$ BEGIN
 IF (SELECT count(*) FROM gold.fact_sales_events) <> 299400
 OR (SELECT count(*) FROM gold.fact_orders) <> 74850 THEN
   RAISE EXCEPTION 'Unexpected fixture counts'; END IF;
 IF (SELECT sum(gross_paid_vnd) FROM gold.order_payment_metrics)
    IS DISTINCT FROM
    (SELECT sum(order_total_vnd) FROM silver.sales_events WHERE event_type='PAYMENT_CONFIRMED')
 THEN RAISE EXCEPTION 'Payment totals mismatch'; END IF;
 IF (SELECT sum(refunded_vnd) FROM gold.order_payment_metrics)
    IS DISTINCT FROM
    (SELECT sum(order_total_vnd) FROM silver.sales_events WHERE event_type='REFUND_ISSUED')
 THEN RAISE EXCEPTION 'Refund totals mismatch'; END IF;
 IF (SELECT event_time_utc FROM gold.fact_sales_events WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f')
    <> timestamptz '2026-08-29 15:16:45+00'
 THEN RAISE EXCEPTION 'UTC reconstruction mismatch'; END IF;
END $$;'''), flush=True)
        bad_cases=[
            ("ALTER TABLE silver.sales_events DROP CONSTRAINT sales_events_pkey; INSERT INTO silver.sales_events SELECT * FROM silver.sales_events LIMIT 1;",'duplicate event_id'),
            ("UPDATE silver.sales_events SET event_date_utc='20263102' WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f';",'date field value out of range'),
            ("UPDATE silver.sales_events SET quantity=NULL WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f';",'invalid required field'),
            ("DELETE FROM silver.sales_events WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f';",'reconciliation failed'),
        ]
        for mutation,error in bad_cases:
            query('BEGIN; SET LOCAL TIME ZONE \'UTC\'; '+mutation+' CALL gold.load_from_silver(); ROLLBACK;',error)
            print('reject:',error,'PASS',flush=True)
        # Correct late arrival: event time takes precedence over producer arrival time.
        query('''BEGIN; SET LOCAL TIME ZONE 'UTC';
UPDATE bronze.raw_sales SET producer_time_utc='2026-09-05 12:00:00'
 WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f';
UPDATE silver.sales_events SET producer_date_utc='20260509',producer_time_utc='12:00:00'
 WHERE event_id='8efe05a2-6874-4d32-a528-992f9fbd099f';
CALL gold.load_from_silver();
DO $$ BEGIN
 IF (SELECT latest_event_type FROM gold.fact_orders WHERE order_id='ORD-00000001') <> 'ORDER_DELIVERED'
 THEN RAISE EXCEPTION 'Late arrival regressed order state'; END IF;
END $$;
ROLLBACK;''')
        print('late arrival preserves event-time state PASS',flush=True)
        # Reinstall migration and loader to verify rerunnable setup.
        for name in ['06_review_gold_schema.sql','07_install_gold_loader.sql']:
            query((ROOT/'sql'/name).read_text())
        print('repeat installation PASS',flush=True)
    finally:
        if started:
            result=run([PG/'pg_ctl','-D',cluster/'data','-m','fast','-w','stop'])
            if result.returncode: raise RuntimeError(result.stdout)
print('Temporary PostgreSQL stopped and removed. ALL TESTS PASSED',flush=True)
