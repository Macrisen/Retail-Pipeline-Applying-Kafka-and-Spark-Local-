-- A failure rolls back this entire load; resolve the error before rerunning.
BEGIN;
SET LOCAL TIME ZONE 'UTC';
CALL gold.load_from_silver();
SELECT 'dim_date' AS table_name,count(*) AS row_count FROM gold.dim_date
UNION ALL SELECT 'dim_customer',count(*) FROM gold.dim_customer
UNION ALL SELECT 'dim_product',count(*) FROM gold.dim_product
UNION ALL SELECT 'fact_sales_events',count(*) FROM gold.fact_sales_events
UNION ALL SELECT 'fact_orders',count(*) FROM gold.fact_orders;
SELECT sum(gross_paid_vnd) AS gross_paid_vnd,
       sum(refunded_vnd) AS refunded_vnd,
       sum(net_paid_vnd) AS net_paid_vnd
FROM gold.order_payment_metrics;
COMMIT;
