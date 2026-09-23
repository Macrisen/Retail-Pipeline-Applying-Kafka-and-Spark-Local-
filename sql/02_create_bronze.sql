-- Raw CSV column order is preserved; duplicates are retained.
-- Source timestamps are UTC, stored without a timezone as in the supplied DDL.

CREATE TABLE IF NOT EXISTS bronze.raw_sales (
    event_id          VARCHAR(255),
    order_id          VARCHAR(255),
    event_type        VARCHAR(255),
    event_time_utc    TIMESTAMP,
    producer_time_utc TIMESTAMP,
    customer_id       VARCHAR(255),
    product_id        VARCHAR(255),
    category          VARCHAR(255),
    quantity          INT,
    unit_price_vnd    DECIMAL(18, 2),
    order_total_vnd   DECIMAL(18, 2),
    payment_method    VARCHAR(255),
    city              VARCHAR(255),
    device            VARCHAR(255),
    traffic_source    VARCHAR(255),
    promo_code        VARCHAR(255),
    is_late_event     BOOLEAN
);
