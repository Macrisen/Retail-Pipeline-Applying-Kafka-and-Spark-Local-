-- Final schema after the supplied timestamp-splitting migration.
-- For a new database; IF NOT EXISTS does not migrate an older existing table.
CREATE TABLE IF NOT EXISTS silver.sales_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_time_utc TIME NOT NULL,
    producer_time_utc TIME,
    customer_id TEXT,
    product_id TEXT,
    category TEXT,
    quantity INT,
    unit_price_vnd NUMERIC(18,2),
    order_total_vnd NUMERIC(18,2),
    payment_method TEXT,
    city TEXT,
    device TEXT,
    traffic_source TEXT,
    promo_code TEXT,
    is_late_event BOOLEAN,
    event_date_utc CHAR(8),
    producer_date_utc CHAR(8),
    loaded_date_utc CHAR(8) DEFAULT to_char(CURRENT_TIMESTAMP AT TIME ZONE 'UTC','YYYYDDMM'),
    loaded_time_utc TIME DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::time
);
CREATE INDEX IF NOT EXISTS idx_sales_events_order ON silver.sales_events(order_id);
CREATE INDEX IF NOT EXISTS idx_sales_events_time ON silver.sales_events(event_time_utc);
