-- Combined form of the supplied initial load and date/time migration.
-- Insert-only event IDs, as in the original ON CONFLICT DO NOTHING rule.
BEGIN;
SET LOCAL TIME ZONE 'UTC';
LOCK TABLE bronze.raw_sales IN SHARE MODE;
WITH cte_convert_format AS (
    SELECT
        event_id::text AS event_id, order_id::text AS order_id,
        event_type::text AS event_type,
        event_time_utc AT TIME ZONE 'UTC' AS event_timestamp,
        producer_time_utc AT TIME ZONE 'UTC' AS producer_timestamp,
        customer_id::text AS customer_id, product_id::text AS product_id,
        category::text AS category, quantity::int AS quantity,
        unit_price_vnd::numeric(18,2) AS unit_price_vnd,
        order_total_vnd::numeric(18,2) AS order_total_vnd,
        payment_method::text AS payment_method, city::text AS city,
        device::text AS device, traffic_source::text AS traffic_source,
        NULLIF(trim(promo_code::text),'') AS promo_code,
        is_late_event::boolean AS is_late_event
    FROM bronze.raw_sales
), cte_remove_duplicate AS (
    SELECT *, row_number() OVER (
        PARTITION BY event_id ORDER BY producer_timestamp DESC NULLS LAST
    ) AS rn
    FROM cte_convert_format
)
INSERT INTO silver.sales_events (
    event_id,order_id,event_type,event_time_utc,producer_time_utc,
    customer_id,product_id,category,quantity,unit_price_vnd,order_total_vnd,
    payment_method,city,device,traffic_source,promo_code,is_late_event,
    event_date_utc,producer_date_utc
)
SELECT event_id,order_id,event_type,
    (event_timestamp AT TIME ZONE 'UTC')::time,
    (producer_timestamp AT TIME ZONE 'UTC')::time,
    customer_id,product_id,category,quantity,unit_price_vnd,order_total_vnd,
    payment_method,city,device,traffic_source,promo_code,is_late_event,
    to_char(event_timestamp AT TIME ZONE 'UTC','YYYYDDMM'),
    to_char(producer_timestamp AT TIME ZONE 'UTC','YYYYDDMM')
FROM cte_remove_duplicate WHERE rn=1
ON CONFLICT(event_id) DO NOTHING;
-- Load date/time defaults apply only to new events; reruns keep original metadata.
COMMIT;
