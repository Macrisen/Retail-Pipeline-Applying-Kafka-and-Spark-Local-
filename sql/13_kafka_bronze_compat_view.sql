CREATE VIEW bronze.raw_sales AS
WITH decoded AS (
    SELECT convert_from(raw_value, 'UTF8')::jsonb AS p
    FROM bronze.kafka_sales_events
    WHERE source_cluster = 'retail-local'
      AND topic = 'retail-order-events'
)
SELECT
    p ->> 'event_id' AS event_id,
    p ->> 'order_id' AS order_id,
    p ->> 'event_type' AS event_type,
    (p ->> 'event_time_utc')::timestamptz AS event_time_utc,
    (p ->> 'producer_time_utc')::timestamptz AS producer_time_utc,
    p ->> 'customer_id' AS customer_id,
    p ->> 'product_id' AS product_id,
    p ->> 'category' AS category,
    (p ->> 'quantity')::integer AS quantity,
    (p ->> 'unit_price_vnd')::numeric(18,2) AS unit_price_vnd,
    (p ->> 'order_total_vnd')::numeric(18,2) AS order_total_vnd,
    p ->> 'payment_method' AS payment_method,
    p ->> 'city' AS city,
    p ->> 'device' AS device,
    p ->> 'traffic_source' AS traffic_source,
    p ->> 'promo_code' AS promo_code,
    (p ->> 'is_late_event')::boolean AS is_late_event
FROM decoded;