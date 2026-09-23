BEGIN;
SET LOCAL TIME ZONE 'UTC';

-- Giữ nguồn ổn định trong lúc nạp.
LOCK TABLE bronze.kafka_sales_events IN SHARE MODE;

-- Đọc JSON gốc từ Bronze.
-- JSON lỗi hoặc giá trị không cast được sẽ làm transaction thất bại.
CREATE TEMP TABLE kafka_silver_source ON COMMIT DROP AS
WITH decoded AS (
    SELECT
        convert_from(raw_value, 'UTF8')::jsonb AS payload,
        source_topic_id,
        partition_id,
        kafka_offset
    FROM bronze.kafka_sales_events
    WHERE source_cluster = 'retail-local'
      AND topic = 'retail-order-events'
)
SELECT
    payload ->> 'event_id' AS event_id,
    payload ->> 'order_id' AS order_id,
    payload ->> 'event_type' AS event_type,

    (payload ->> 'event_time_utc')::timestamptz
        AT TIME ZONE 'UTC' AS event_timestamp,

    (payload ->> 'producer_time_utc')::timestamptz
        AT TIME ZONE 'UTC' AS producer_timestamp,

    payload ->> 'customer_id' AS customer_id,
    payload ->> 'product_id' AS product_id,
    payload ->> 'category' AS category,

    (payload ->> 'quantity')::integer AS quantity,
    (payload ->> 'unit_price_vnd')::numeric(18,2) AS unit_price_vnd,
    (payload ->> 'order_total_vnd')::numeric(18,2) AS order_total_vnd,

    payload ->> 'payment_method' AS payment_method,
    payload ->> 'city' AS city,
    payload ->> 'device' AS device,
    payload ->> 'traffic_source' AS traffic_source,

    NULLIF(trim(payload ->> 'promo_code'), '') AS promo_code,
    (payload ->> 'is_late_event')::boolean AS is_late_event,

    source_topic_id,
    partition_id,
    kafka_offset
FROM decoded;

-- Kiểm tra cơ bản trước khi ghi vào Silver.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM kafka_silver_source) THEN
        RAISE EXCEPTION 'Không có dữ liệu từ topic chính trong Bronze';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM kafka_silver_source
        WHERE NULLIF(trim(event_id), '') IS NULL
           OR NULLIF(trim(order_id), '') IS NULL
           OR NULLIF(trim(event_type), '') IS NULL
           OR event_timestamp IS NULL
           OR quantity IS NULL
           OR quantity <= 0
           OR unit_price_vnd IS NULL
           OR unit_price_vnd < 0
           OR unit_price_vnd::text IN ('NaN', 'Infinity', '-Infinity')
           OR order_total_vnd IS NULL
           OR order_total_vnd < 0
           OR order_total_vnd::text IN ('NaN', 'Infinity', '-Infinity')
    ) THEN
        RAISE EXCEPTION 'Có dữ liệu thiếu trường bắt buộc hoặc số không hợp lệ';
    END IF;
END;
$$;

-- Mỗi event_id chỉ lấy một bản.
WITH ranked AS (
    SELECT *,
        row_number() OVER (
            PARTITION BY event_id
            ORDER BY producer_timestamp DESC NULLS LAST,
                     source_topic_id,
                     partition_id,
                     kafka_offset DESC
        ) AS rn
    FROM kafka_silver_source
)
INSERT INTO silver.sales_events (
    event_id, order_id, event_type,
    event_time_utc, producer_time_utc,
    customer_id, product_id, category,
    quantity, unit_price_vnd, order_total_vnd,
    payment_method, city, device, traffic_source,
    promo_code, is_late_event,
    event_date_utc, producer_date_utc
)
SELECT
    event_id, order_id, event_type,
    event_timestamp::time,
    producer_timestamp::time,
    customer_id, product_id, category,
    quantity, unit_price_vnd, order_total_vnd,
    payment_method, city, device, traffic_source,
    promo_code, is_late_event,
    to_char(event_timestamp, 'YYYYDDMM'),
    to_char(producer_timestamp, 'YYYYDDMM')
FROM ranked
WHERE rn = 1
ON CONFLICT (event_id) DO NOTHING;

COMMIT;