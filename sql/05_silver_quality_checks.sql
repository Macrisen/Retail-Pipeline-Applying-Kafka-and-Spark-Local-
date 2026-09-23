-- Run once to install the reusable validation/staging procedure.
-- Source schema names follow RETAIL_DATA_ENGINEERING_PROJECT_PLAN.md.
CREATE OR REPLACE PROCEDURE silver.prepare_gold_source()
LANGUAGE plpgsql AS $$
BEGIN
    -- Keep Bronze/Silver stable without changing isolation after editor queries.
    LOCK TABLE bronze.raw_sales, silver.sales_events IN SHARE MODE;
    DROP TABLE IF EXISTS pg_temp.retail_source;
    DROP TABLE IF EXISTS pg_temp.retail_bronze;

    -- make_date rejects impossible dates rather than silently normalizing them.
    IF EXISTS (
        SELECT 1 FROM silver.sales_events
        WHERE event_date_utc::text !~ '^[0-9]{8}$'
           OR producer_date_utc::text !~ '^[0-9]{8}$'
           OR loaded_date_utc::text !~ '^[0-9]{8}$'
           OR event_date_utc IS NULL OR producer_date_utc IS NULL
           OR loaded_date_utc IS NULL OR loaded_time_utc IS NULL
    ) THEN
        RAISE EXCEPTION 'Silver: dates must be non-null YYYYDDMM; load time must be present';
    END IF;

    CREATE TEMP TABLE retail_source ON COMMIT DROP AS
    SELECT
        event_id::text AS event_id, order_id::text AS order_id,
        event_type::text AS event_type,
        (make_date(substring(event_date_utc::text,1,4)::int,
                   substring(event_date_utc::text,7,2)::int,
                   substring(event_date_utc::text,5,2)::int)
            + event_time_utc::time) AT TIME ZONE 'UTC' AS event_time_utc,
        (make_date(substring(producer_date_utc::text,1,4)::int,
                   substring(producer_date_utc::text,7,2)::int,
                   substring(producer_date_utc::text,5,2)::int)
            + producer_time_utc::time) AT TIME ZONE 'UTC' AS producer_time_utc,
        customer_id::text AS customer_id, product_id::text AS product_id,
        category::text AS category, quantity::int AS quantity,
        unit_price_vnd::numeric(18,2) AS unit_price_vnd,
        order_total_vnd::numeric(18,2) AS order_total_vnd,
        payment_method::text AS payment_method, city::text AS city,
        device::text AS device, traffic_source::text AS traffic_source,
        NULLIF(NULLIF(btrim(promo_code::text), ''), 'null') AS promo_code,
        is_late_event::text::boolean AS is_late_event
    FROM silver.sales_events;

    -- Also validate the calendar and TIME values of load metadata.
    PERFORM make_date(substring(loaded_date_utc::text,1,4)::int,
                      substring(loaded_date_utc::text,7,2)::int,
                      substring(loaded_date_utc::text,5,2)::int)
                + loaded_time_utc::time
    FROM silver.sales_events;

    IF NOT EXISTS (SELECT 1 FROM retail_source) THEN
        RAISE EXCEPTION 'Silver is empty; no Gold data loaded';
    END IF;
    IF EXISTS (SELECT event_id FROM retail_source GROUP BY event_id HAVING count(*) > 1) THEN
        RAISE EXCEPTION 'Silver contains duplicate event_id';
    END IF;
    IF EXISTS (
        SELECT 1 FROM retail_source s
        WHERE EXISTS (
            SELECT 1 FROM unnest(ARRAY[s.event_id,s.order_id,s.event_type,
                s.customer_id,s.product_id,s.category]) AS x(v)
            WHERE v IS NULL OR btrim(v) = '' OR lower(btrim(v)) = 'null'
               OR v <> btrim(v)
        ) OR event_time_utc IS NULL OR producer_time_utc IS NULL
          OR quantity IS NULL OR quantity <= 0
          OR unit_price_vnd IS NULL OR unit_price_vnd < 0
          OR unit_price_vnd::text IN ('NaN','Infinity','-Infinity')
          OR order_total_vnd IS NULL OR order_total_vnd < 0
          OR order_total_vnd::text IN ('NaN','Infinity','-Infinity')
          OR is_late_event IS NULL
          OR producer_time_utc < event_time_utc
          OR event_type NOT IN ('ORDER_CREATED','PAYMENT_CONFIRMED','ORDER_SHIPPED',
              'ORDER_DELIVERED','ORDER_CANCELLED','REFUND_ISSUED','CANCEL_ACKNOWLEDGED')
    ) THEN
        RAISE EXCEPTION 'Silver: invalid required field, number, timestamp, or event type';
    END IF;

    -- A single order amount/customer/product is required by this dataset model.
    IF EXISTS (
        SELECT order_id FROM retail_source
        GROUP BY order_id
        HAVING count(DISTINCT ROW(customer_id,product_id,category,quantity,
                     unit_price_vnd,order_total_vnd,payment_method,city,device,
                     traffic_source,promo_code)) > 1
    ) THEN
        RAISE EXCEPTION 'Order attributes conflict: review order grain before loading Gold';
    END IF;
    IF EXISTS (
        SELECT order_id FROM retail_source GROUP BY order_id
        HAVING count(*) FILTER (WHERE event_type='PAYMENT_CONFIRMED') > 1
            OR count(*) FILTER (WHERE event_type='REFUND_ISSUED') > 1
            OR (count(*) FILTER (WHERE event_type='REFUND_ISSUED') > 0
                AND count(*) FILTER (WHERE event_type='PAYMENT_CONFIRMED') = 0)
    ) THEN
        RAISE EXCEPTION 'Payment/refund pattern requires a different business rule';
    END IF;

    CREATE TEMP TABLE retail_bronze ON COMMIT DROP AS
    SELECT DISTINCT
        event_id::text AS event_id, order_id::text AS order_id,
        event_type::text AS event_type,
        event_time_utc::timestamptz AS event_time_utc,
        producer_time_utc::timestamptz AS producer_time_utc,
        customer_id::text AS customer_id, product_id::text AS product_id,
        category::text AS category, quantity::int AS quantity,
        unit_price_vnd::numeric(18,2) AS unit_price_vnd,
        order_total_vnd::numeric(18,2) AS order_total_vnd,
        payment_method::text AS payment_method, city::text AS city,
        device::text AS device, traffic_source::text AS traffic_source,
        NULLIF(NULLIF(btrim(promo_code::text), ''), 'null') AS promo_code,
        is_late_event::text::boolean AS is_late_event
    FROM bronze.raw_sales;

    IF EXISTS (SELECT event_id FROM retail_bronze GROUP BY event_id HAVING count(*) > 1) THEN
        RAISE EXCEPTION 'Bronze has conflicting versions of the same event_id';
    END IF;
    IF EXISTS (
        (SELECT * FROM retail_source EXCEPT SELECT * FROM retail_bronze)
        UNION ALL
        (SELECT * FROM retail_bronze EXCEPT SELECT * FROM retail_source)
    ) THEN
        RAISE EXCEPTION 'Bronze/Silver reconciliation failed (including UTC date reconstruction)';
    END IF;
    CREATE UNIQUE INDEX ON retail_source(event_id);
    CREATE INDEX ON retail_source(order_id);
    ANALYZE retail_source;
    RAISE NOTICE 'Silver PASS: % unique events, % orders; Bronze % rows',
        (SELECT count(*) FROM retail_source),
        (SELECT count(DISTINCT order_id) FROM retail_source),
        (SELECT count(*) FROM bronze.raw_sales);
END;
$$;
REVOKE ALL ON PROCEDURE silver.prepare_gold_source() FROM PUBLIC;

-- Install only. Run CALL silver.prepare_gold_source() after Silver has data,
-- or use 08, whose Gold loader calls this validator before writing Gold.
