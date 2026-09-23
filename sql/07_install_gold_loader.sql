BEGIN;
-- Install loader; writes happen only when CALL is executed in script 08.
CREATE OR REPLACE PROCEDURE gold.load_from_silver()
LANGUAGE plpgsql AS $$
BEGIN
    -- Serialize this batch loader. Source tables are locked by prepare_gold_source().
    PERFORM pg_advisory_xact_lock(74850,299400);
    PERFORM set_config('TimeZone','UTC',true);
    CALL silver.prepare_gold_source();

    INSERT INTO gold.dim_date AS d
        (date_key,full_date,date_label,day_of_month,month_number,
         quarter_number,year_number,day_of_week,is_weekend)
    SELECT to_char(v,'YYYYMMDD')::int, v::date, to_char(v,'YYYYDDMM'),
        extract(day FROM v)::smallint, extract(month FROM v)::smallint,
        extract(quarter FROM v)::smallint, extract(year FROM v)::smallint,
        extract(isodow FROM v)::smallint, extract(isodow FROM v) IN (6,7)
    FROM generate_series(
        (SELECT min(event_time_utc AT TIME ZONE 'UTC')::date FROM retail_source)::timestamp,
        (SELECT greatest(max(event_time_utc),max(producer_time_utc)) AT TIME ZONE 'UTC'
         FROM retail_source)::date::timestamp,
        interval '1 day') AS dates(v)
    ON CONFLICT(date_key) DO UPDATE SET
        full_date=excluded.full_date,date_label=excluded.date_label,
        day_of_month=excluded.day_of_month,month_number=excluded.month_number,
        quarter_number=excluded.quarter_number,year_number=excluded.year_number,
        day_of_week=excluded.day_of_week,is_weekend=excluded.is_weekend
    -- NULL-safe comparison: unchanged rows do not create new tuple versions.
    WHERE ROW(d.full_date, d.date_label, d.day_of_month, d.month_number, d.quarter_number, d.year_number, d.day_of_week, d.is_weekend)
        IS DISTINCT FROM ROW(excluded.full_date, excluded.date_label, excluded.day_of_month, excluded.month_number, excluded.quarter_number, excluded.year_number, excluded.day_of_week, excluded.is_weekend);

    -- Exclude existing business keys before inserting, preserving identity sequences on rerun.
    INSERT INTO gold.dim_customer(customer_id)
    SELECT DISTINCT s.customer_id FROM retail_source s
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_customer d WHERE d.customer_id=s.customer_id)
    ORDER BY s.customer_id
    ON CONFLICT(customer_id) DO NOTHING;

    INSERT INTO gold.dim_product(product_id,category)
    SELECT DISTINCT s.product_id,s.category FROM retail_source s
    WHERE NOT EXISTS (SELECT 1 FROM gold.dim_product d
                      WHERE d.product_id=s.product_id AND d.category=s.category)
    ORDER BY s.product_id,s.category
    ON CONFLICT(product_id,category) DO NOTHING;

    DROP TABLE IF EXISTS pg_temp.retail_expected_events;
    CREATE TEMP TABLE retail_expected_events ON COMMIT DROP AS
    SELECT s.event_id,s.order_id,s.event_type,
        to_char(s.event_time_utc AT TIME ZONE 'UTC','YYYYMMDD')::int AS event_date_key,
        to_char(s.producer_time_utc AT TIME ZONE 'UTC','YYYYMMDD')::int AS producer_date_key,
        c.customer_key,p.product_key,s.event_time_utc,s.producer_time_utc,
        s.quantity,s.unit_price_vnd,s.order_total_vnd,s.payment_method,
        s.city,s.device,s.traffic_source,s.promo_code,s.is_late_event
    FROM retail_source s
    JOIN gold.dim_customer c ON c.customer_id=s.customer_id
    JOIN gold.dim_product p ON p.product_id=s.product_id AND p.category=s.category;

    IF (SELECT count(*) FROM retail_expected_events) <> (SELECT count(*) FROM retail_source) THEN
        RAISE EXCEPTION 'Dimension joins lost or multiplied source rows';
    END IF;

    DROP TABLE IF EXISTS pg_temp.retail_expected_orders;
    CREATE TEMP TABLE retail_expected_orders ON COMMIT DROP AS
    WITH milestones AS (
        SELECT order_id,
            min(event_time_utc) FILTER(WHERE event_type='ORDER_CREATED') AS created_at_utc,
            min(event_time_utc) FILTER(WHERE event_type='PAYMENT_CONFIRMED') AS paid_at_utc,
            min(event_time_utc) FILTER(WHERE event_type='ORDER_SHIPPED') AS shipped_at_utc,
            min(event_time_utc) FILTER(WHERE event_type='ORDER_DELIVERED') AS delivered_at_utc
        FROM retail_source GROUP BY order_id
    ), latest AS (
        SELECT DISTINCT ON(order_id) * FROM retail_source
        -- Event time wins over arrival time; ties have deterministic ordering.
        ORDER BY order_id,event_time_utc DESC,producer_time_utc DESC,event_id DESC
    )
    SELECT l.order_id,c.customer_key,
        to_char(m.created_at_utc AT TIME ZONE 'UTC','YYYYMMDD')::int AS created_date_key,
        to_char(m.paid_at_utc AT TIME ZONE 'UTC','YYYYMMDD')::int AS paid_date_key,
        to_char(m.delivered_at_utc AT TIME ZONE 'UTC','YYYYMMDD')::int AS delivered_date_key,
        m.created_at_utc,m.paid_at_utc,m.shipped_at_utc,m.delivered_at_utc,
        l.event_type AS latest_event_type,l.event_time_utc AS latest_event_at_utc,
        l.order_total_vnd,l.payment_method,l.city,l.device,l.traffic_source,l.promo_code
    FROM latest l JOIN milestones m USING(order_id)
    JOIN gold.dim_customer c ON c.customer_id=l.customer_id;

    INSERT INTO gold.fact_sales_events AS target (event_id,order_id,event_type,event_date_key,producer_date_key,customer_key,product_key,event_time_utc,producer_time_utc,quantity,unit_price_vnd,order_total_vnd,payment_method,city,device,traffic_source,promo_code,is_late_event)
    SELECT event_id,order_id,event_type,event_date_key,producer_date_key,customer_key,product_key,event_time_utc,producer_time_utc,quantity,unit_price_vnd,order_total_vnd,payment_method,city,device,traffic_source,promo_code,is_late_event FROM retail_expected_events
    ON CONFLICT(event_id) DO UPDATE SET
        order_id=excluded.order_id,
        event_type=excluded.event_type,
        event_date_key=excluded.event_date_key,
        producer_date_key=excluded.producer_date_key,
        customer_key=excluded.customer_key,
        product_key=excluded.product_key,
        event_time_utc=excluded.event_time_utc,
        producer_time_utc=excluded.producer_time_utc,
        quantity=excluded.quantity,
        unit_price_vnd=excluded.unit_price_vnd,
        order_total_vnd=excluded.order_total_vnd,
        payment_method=excluded.payment_method,
        city=excluded.city,
        device=excluded.device,
        traffic_source=excluded.traffic_source,
        promo_code=excluded.promo_code,
        is_late_event=excluded.is_late_event
    -- NULL-safe comparison: unchanged rows do not create new tuple versions.
    WHERE ROW(target.order_id, target.event_type, target.event_date_key, target.producer_date_key, target.customer_key, target.product_key, target.event_time_utc, target.producer_time_utc, target.quantity, target.unit_price_vnd, target.order_total_vnd, target.payment_method, target.city, target.device, target.traffic_source, target.promo_code, target.is_late_event)
        IS DISTINCT FROM ROW(excluded.order_id, excluded.event_type, excluded.event_date_key, excluded.producer_date_key, excluded.customer_key, excluded.product_key, excluded.event_time_utc, excluded.producer_time_utc, excluded.quantity, excluded.unit_price_vnd, excluded.order_total_vnd, excluded.payment_method, excluded.city, excluded.device, excluded.traffic_source, excluded.promo_code, excluded.is_late_event);

    IF EXISTS (
        (SELECT event_id,order_id,event_type,event_date_key,producer_date_key,customer_key,product_key,event_time_utc,producer_time_utc,quantity,unit_price_vnd,order_total_vnd,payment_method,city,device,traffic_source,promo_code,is_late_event FROM gold.fact_sales_events EXCEPT SELECT event_id,order_id,event_type,event_date_key,producer_date_key,customer_key,product_key,event_time_utc,producer_time_utc,quantity,unit_price_vnd,order_total_vnd,payment_method,city,device,traffic_source,promo_code,is_late_event FROM retail_expected_events)
        UNION ALL
        (SELECT event_id,order_id,event_type,event_date_key,producer_date_key,customer_key,product_key,event_time_utc,producer_time_utc,quantity,unit_price_vnd,order_total_vnd,payment_method,city,device,traffic_source,promo_code,is_late_event FROM retail_expected_events EXCEPT SELECT event_id,order_id,event_type,event_date_key,producer_date_key,customer_key,product_key,event_time_utc,producer_time_utc,quantity,unit_price_vnd,order_total_vnd,payment_method,city,device,traffic_source,promo_code,is_late_event FROM gold.fact_sales_events)
    ) THEN
        RAISE EXCEPTION 'fact_sales_events reconciliation failed; stale rows or inconsistent values';
    END IF;

    INSERT INTO gold.fact_orders AS target (order_id,customer_key,created_date_key,paid_date_key,delivered_date_key,created_at_utc,paid_at_utc,shipped_at_utc,delivered_at_utc,latest_event_type,latest_event_at_utc,order_total_vnd,payment_method,city,device,traffic_source,promo_code)
    SELECT order_id,customer_key,created_date_key,paid_date_key,delivered_date_key,created_at_utc,paid_at_utc,shipped_at_utc,delivered_at_utc,latest_event_type,latest_event_at_utc,order_total_vnd,payment_method,city,device,traffic_source,promo_code FROM retail_expected_orders
    ON CONFLICT(order_id) DO UPDATE SET
        customer_key=excluded.customer_key,
        created_date_key=excluded.created_date_key,
        paid_date_key=excluded.paid_date_key,
        delivered_date_key=excluded.delivered_date_key,
        created_at_utc=excluded.created_at_utc,
        paid_at_utc=excluded.paid_at_utc,
        shipped_at_utc=excluded.shipped_at_utc,
        delivered_at_utc=excluded.delivered_at_utc,
        latest_event_type=excluded.latest_event_type,
        latest_event_at_utc=excluded.latest_event_at_utc,
        order_total_vnd=excluded.order_total_vnd,
        payment_method=excluded.payment_method,
        city=excluded.city,
        device=excluded.device,
        traffic_source=excluded.traffic_source,
        promo_code=excluded.promo_code
    -- NULL-safe comparison: unchanged rows do not create new tuple versions.
    WHERE ROW(target.customer_key, target.created_date_key, target.paid_date_key, target.delivered_date_key, target.created_at_utc, target.paid_at_utc, target.shipped_at_utc, target.delivered_at_utc, target.latest_event_type, target.latest_event_at_utc, target.order_total_vnd, target.payment_method, target.city, target.device, target.traffic_source, target.promo_code)
        IS DISTINCT FROM ROW(excluded.customer_key, excluded.created_date_key, excluded.paid_date_key, excluded.delivered_date_key, excluded.created_at_utc, excluded.paid_at_utc, excluded.shipped_at_utc, excluded.delivered_at_utc, excluded.latest_event_type, excluded.latest_event_at_utc, excluded.order_total_vnd, excluded.payment_method, excluded.city, excluded.device, excluded.traffic_source, excluded.promo_code);

    IF EXISTS (
        (SELECT order_id,customer_key,created_date_key,paid_date_key,delivered_date_key,created_at_utc,paid_at_utc,shipped_at_utc,delivered_at_utc,latest_event_type,latest_event_at_utc,order_total_vnd,payment_method,city,device,traffic_source,promo_code FROM gold.fact_orders EXCEPT SELECT order_id,customer_key,created_date_key,paid_date_key,delivered_date_key,created_at_utc,paid_at_utc,shipped_at_utc,delivered_at_utc,latest_event_type,latest_event_at_utc,order_total_vnd,payment_method,city,device,traffic_source,promo_code FROM retail_expected_orders)
        UNION ALL
        (SELECT order_id,customer_key,created_date_key,paid_date_key,delivered_date_key,created_at_utc,paid_at_utc,shipped_at_utc,delivered_at_utc,latest_event_type,latest_event_at_utc,order_total_vnd,payment_method,city,device,traffic_source,promo_code FROM retail_expected_orders EXCEPT SELECT order_id,customer_key,created_date_key,paid_date_key,delivered_date_key,created_at_utc,paid_at_utc,shipped_at_utc,delivered_at_utc,latest_event_type,latest_event_at_utc,order_total_vnd,payment_method,city,device,traffic_source,promo_code FROM gold.fact_orders)
    ) THEN
        RAISE EXCEPTION 'fact_orders reconciliation failed; stale rows or inconsistent values';
    END IF;

    RAISE NOTICE 'Gold PASS: % event facts, % order facts',
        (SELECT count(*) FROM gold.fact_sales_events), (SELECT count(*) FROM gold.fact_orders);
END;
$$;
REVOKE ALL ON PROCEDURE gold.load_from_silver() FROM PUBLIC;

-- These are synthetic payment-flow metrics, not accounting revenue recognition.
-- Full refund assumption: REFUND_ISSUED refunds the entire order amount.
CREATE OR REPLACE VIEW gold.order_payment_metrics AS
WITH flags AS (
    SELECT order_id,
        bool_or(event_type='PAYMENT_CONFIRMED') AS has_payment,
        bool_or(event_type='REFUND_ISSUED') AS has_refund,
        bool_or(event_type IN ('ORDER_CANCELLED','CANCEL_ACKNOWLEDGED')) AS is_cancelled
    FROM gold.fact_sales_events GROUP BY order_id
)
SELECT o.order_id,o.paid_date_key,o.order_total_vnd,
    f.has_payment,f.has_refund,f.is_cancelled,
    CASE WHEN f.has_payment THEN o.order_total_vnd ELSE 0 END AS gross_paid_vnd,
    CASE WHEN f.has_refund THEN o.order_total_vnd ELSE 0 END AS refunded_vnd,
    CASE WHEN f.has_payment THEN o.order_total_vnd ELSE 0 END
        - CASE WHEN f.has_refund THEN o.order_total_vnd ELSE 0 END AS net_paid_vnd
FROM gold.fact_orders o JOIN flags f USING(order_id);
COMMIT;
