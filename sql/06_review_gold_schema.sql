BEGIN;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INT PRIMARY KEY, full_date DATE NOT NULL UNIQUE,
    date_label VARCHAR(8) NOT NULL, day_of_month SMALLINT NOT NULL,
    month_number SMALLINT NOT NULL, quarter_number SMALLINT NOT NULL,
    year_number SMALLINT NOT NULL, day_of_week SMALLINT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id TEXT NOT NULL, category TEXT NOT NULL,
    CONSTRAINT uq_dim_product_product_category UNIQUE(product_id,category)
);
-- Safe to rerun after the migration supplied by the user.
ALTER TABLE gold.dim_product DROP CONSTRAINT IF EXISTS dim_product_product_id_key;
ALTER TABLE gold.dim_product ALTER COLUMN category SET NOT NULL;
DO $$ BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
        WHERE conrelid='gold.dim_product'::regclass
          AND conname='uq_dim_product_product_category') THEN
        ALTER TABLE gold.dim_product ADD CONSTRAINT uq_dim_product_product_category
            UNIQUE(product_id,category);
    END IF;
END $$;
CREATE TABLE IF NOT EXISTS gold.fact_sales_events (
    event_id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    event_date_key INT NOT NULL REFERENCES gold.dim_date(date_key),
    producer_date_key INT REFERENCES gold.dim_date(date_key),
    customer_key BIGINT REFERENCES gold.dim_customer(customer_key),
    product_key BIGINT REFERENCES gold.dim_product(product_key),
    event_time_utc TIMESTAMPTZ NOT NULL,
    producer_time_utc TIMESTAMPTZ,
    quantity INT,
    unit_price_vnd NUMERIC(18,2),
    order_total_vnd NUMERIC(18,2),
    payment_method TEXT,
    city TEXT,
    device TEXT,
    traffic_source TEXT,
    promo_code TEXT,
    is_late_event BOOLEAN
);
CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_id TEXT PRIMARY KEY,
    customer_key BIGINT REFERENCES gold.dim_customer(customer_key),
    created_date_key INT REFERENCES gold.dim_date(date_key),
    paid_date_key INT REFERENCES gold.dim_date(date_key),
    delivered_date_key INT REFERENCES gold.dim_date(date_key),
    created_at_utc TIMESTAMPTZ,
    paid_at_utc TIMESTAMPTZ,
    shipped_at_utc TIMESTAMPTZ,
    delivered_at_utc TIMESTAMPTZ,
    latest_event_type TEXT NOT NULL,
    latest_event_at_utc TIMESTAMPTZ NOT NULL,
    order_total_vnd NUMERIC(18,2),
    payment_method TEXT,
    city TEXT,
    device TEXT,
    traffic_source TEXT,
    promo_code TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_sales_events_order ON gold.fact_sales_events(order_id);
CREATE INDEX IF NOT EXISTS idx_fact_sales_events_date ON gold.fact_sales_events(event_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_created_date ON gold.fact_orders(created_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_paid_date ON gold.fact_orders(paid_date_key);
DO $$ BEGIN
    IF (SELECT count(*) FROM information_schema.columns
        WHERE table_schema='gold' AND data_type='timestamp with time zone'
          AND ((table_name='fact_sales_events' AND column_name IN ('event_time_utc','producer_time_utc'))
           OR (table_name='fact_orders' AND column_name IN ('created_at_utc','paid_at_utc',
                'shipped_at_utc','delivered_at_utc','latest_event_at_utc')))) <> 7 THEN
        RAISE EXCEPTION 'Gold timestamp columns do not match supplied DDL; inspect before loading';
    END IF;
END $$;
COMMIT;
