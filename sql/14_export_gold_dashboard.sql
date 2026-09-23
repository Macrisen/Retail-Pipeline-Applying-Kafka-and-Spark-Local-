-- Run from the project root after creating the dashboard directory.
-- All exports share one consistent database snapshot.
\set ON_ERROR_STOP on
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;
SET LOCAL TIME ZONE 'UTC';
\copy (SELECT * FROM gold.dim_date ORDER BY date_key) TO 'dashboard/dim_date.csv' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
\copy (SELECT * FROM gold.dim_customer ORDER BY customer_key) TO 'dashboard/dim_customer.csv' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
\copy (SELECT * FROM gold.dim_product ORDER BY product_key) TO 'dashboard/dim_product.csv' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
\copy (SELECT * FROM gold.fact_orders ORDER BY order_id) TO 'dashboard/fact_orders.csv' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
\copy (SELECT * FROM gold.fact_sales_events ORDER BY event_id) TO 'dashboard/fact_sales_events.csv' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
\copy (SELECT * FROM gold.order_payment_metrics ORDER BY order_id) TO 'dashboard/order_payment_metrics.csv' WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')
COMMIT;
