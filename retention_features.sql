WITH users AS (
    SELECT 
        _id AS ref_customer_id,
        useruid AS customer_id,
        mobilenumber
    FROM public.users 
    WHERE usertype = 1
),

-- -------------------------------------------
--  CUSTOMER SUPPORT EVENTS (for complaint flag)
-- -------------------------------------------
support AS (
    SELECT
        cust.ref_customer_id,
        TO_TIMESTAMP(s.initiated_at, 'MM/DD/YYYY HH24:MI') AS complaint_ts
    FROM "upload_chats_data_4_months_20251110062008" s
    LEFT JOIN users cust 
        ON s.user_properties_phone_number = cust.mobilenumber
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY cust.ref_customer_id, s.initiated_at
        ORDER BY initiated_at
    ) = 1
),

-- -------------------------------------------
--  BASE COMPLETED JOURNEYS (core features)
-- -------------------------------------------
base AS (
    SELECT
        j.journey_id,
        j.ref_journey_id,
        j.ref_customer_id,
        j.customer_id,

        CASE WHEN ref_vehicle_category_id IN 
             ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
             THEN 'Taxi' ELSE 'Limo' END AS vehicle_cat,

        (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
        DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS journey_dt,
        EXTRACT(HOUR FROM (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS local_booking_hr,
        TRIM(TO_CHAR((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai','Day')) AS weekday,
        case when  journey_status IN (9,10) then 1 else 0 end  Completed_trip,
        case when  journey_status IN (13) then 1 else 0 end  customer_cancelled,
        case when  journey_status IN (14) then 1 else 0 end  driver_cancelled,
        case when  journey_status IN (19,15) then 1 else 0 end  admin_cancelled,
        



        accepted_ride_timestamp,
        on_route_timestamp,
        arrived_at_pickup_timestamp,
        journey_completed_timestamp,
        dispatch_eta,

        pickup_latitude,
        pickup_longitude,
        actual_drop_off_latitude,
        actual_drop_off_longitude,
        estimate_drop_off_latitude,
        estimate_drop_off_longitude,

        j.estimate_total_fee,
        j.actual_total_fee,
        j.actual_distance,
        j.ref_promo_applied,
        j.payment_description

    FROM prod_etl_data.tbl_journey_master j
    WHERE 
    1=1
    -- j.journey_status IN (9,10)
      AND DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-07-01' 
      AND  DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <'2025-11-21'
)
-- select count(*), count(distinct customer_id), count(distinct journey_id )from base;
-- 468,168 119,530

-- -------------------------------------------
--  MAP DROP LOCATION TO HOOD / ZONE
-- -------------------------------------------
,geo AS (
    SELECT 
        b.journey_id,
        h.name AS drop_hood,
        h.zone_name AS drop_zone
        
    FROM base b
    LEFT JOIN prod_etl_temp.tbl_hood_kml_with_zone h 
    ON ST_Contains(
        h.geom,
        ST_SetSRID(ST_Point(b.actual_drop_off_longitude, b.actual_drop_off_latitude), ST_SRID(h.geom))
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY b.journey_id ORDER BY ST_Area(h.geom)) = 1
)
-- select count(*), count(distinct journey_id ) from geo; -- 468168


-- -------------------------------------------
--  PRICE ERROR + DISTANCE ACCURACY
-- -------------------------------------------
,fare AS (
    SELECT
        b.*,
        g.drop_hood,
        g.drop_zone,

        ST_DistanceSphere(
            ST_MakePoint(estimate_drop_off_longitude, estimate_drop_off_latitude),
            ST_MakePoint(actual_drop_off_longitude, actual_drop_off_latitude)
        ) AS drop_dist_m,

        (actual_total_fee - estimate_total_fee) / NULLIF(estimate_total_fee,0) AS price_error,

        CASE 
            WHEN actual_total_fee <= 30 THEN '0.<30'
            WHEN actual_total_fee <= 50 THEN '1.30-50'
            WHEN actual_total_fee <= 70 THEN '2.50-70'
            ELSE '3.>70'
        END AS fee_bucket,

        CASE
            WHEN (actual_total_fee - estimate_total_fee) <= 0 THEN '0.<=0'
            WHEN price_error <= 0.05 THEN '1.0–5%'
            WHEN price_error <= 0.10 THEN '2.5–10%'
            WHEN price_error <= 0.15 THEN '3.10–15%'
            WHEN price_error <= 0.20 THEN '4.15–20%'
            ELSE '5.>20%'
        END AS price_bucket

    FROM base b
    LEFT JOIN geo g USING (journey_id)
)
-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from fare;
-- 468168	468168	119530	


-- -------------------------------------------
--  TIME FEATURES (ETA, ATA)
-- -------------------------------------------
,time_features AS (
    SELECT
        f.*,
        DATEDIFF(
            minute,
            CASE WHEN vehicle_cat = 'Limo' THEN accepted_ride_timestamp ELSE on_route_timestamp END,
            arrived_at_pickup_timestamp
        ) AS ata_min
    FROM fare f
)
-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from time_features;

-- -------------------------------------------
--  CUSTOMER LIFETIME COMPLETED TRIPS (before this journey)
-- -------------------------------------------
,prior_trips AS (
    SELECT
        ref_customer_id,
        journey_id,
        ROW_NUMBER() OVER ( PARTITION BY ref_customer_id ORDER BY journey_created_at) AS trip_seq_num,
        ROW_NUMBER() OVER ( PARTITION BY ref_customer_id ORDER BY journey_created_at ) - 1 AS prior_completed_trips
    FROM prod_etl_data.tbl_journey_master 
    WHERE 
    journey_status IN (9,10)
)

-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from prior_trips;
-- 468168	468168	119530	

-- -------------------------------------------
--  RATINGS
-- -------------------------------------------
,ratings AS (
    SELECT ref_journey_id, rating
    FROM prod_etl_data.tbl_userrating_details
)

-- -------------------------------------------
--  24H COMPLAINT FLAG 
-- -------------------------------------------
,journey_w_complaints AS (
    SELECT 
        t.*,
        CASE WHEN EXISTS (
            SELECT 1
            FROM support s
            WHERE s.ref_customer_id = t.ref_customer_id
              AND s.complaint_ts BETWEEN t.journey_ts AND t.journey_ts + INTERVAL '24 hour'
        ) THEN 1 ELSE 0 END AS complaint_24h
    FROM time_features t
)

-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from journey_w_complaints;
-- ,
-- 468168	468168	119530	

-- -------------------------------------------
--  FUTURE JOURNEYS (RETENTION LABEL)
-- -------------------------------------------
,future_trips AS (
    SELECT
        ref_customer_id,
        journey_ts
    FROM time_features
)

-- ============================================================
-- 1. ORDER ALL COMPLETED TRIPS TO CREATE USER JOURNEY SEQUENCE
-- ============================================================
,ordered AS (
    SELECT
        b.*,

        LAG(journey_ts) OVER (PARTITION BY ref_customer_id ORDER BY journey_ts ) AS prev_trip_ts,

        LAG(ref_promo_applied) OVER ( PARTITION BY ref_customer_id ORDER BY journey_ts ) AS prev_promo,

        LAG(Completed_trip)      OVER (PARTITION BY ref_customer_id ORDER BY journey_ts) AS prev_completed_trip,
        LAG(customer_cancelled)  OVER (PARTITION BY ref_customer_id ORDER BY journey_ts) AS prev_customer_cancelled,
        LAG(driver_cancelled)    OVER (PARTITION BY ref_customer_id ORDER BY journey_ts) AS prev_driver_cancelled,
        LAG(admin_cancelled)     OVER (PARTITION BY ref_customer_id ORDER BY journey_ts) AS prev_admin_cancelled


        -- LAG(payment_description) OVER ( PARTITION BY ref_customer_id ORDER BY journey_ts) AS prev_payment_method
    FROM journey_w_complaints b
)


-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from ordered;
-- 468168	468168	119530	


,overall_status AS (
    SELECT
        ref_customer_id,
        journey_id,

        -- cumulative counts up to and including this journey
        SUM(CASE WHEN journey_status IN (9,10) THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_customer_id ORDER BY journey_created_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_completed_trips,

        SUM(CASE WHEN journey_status = 13 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_customer_id ORDER BY journey_created_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_customer_cancelled,

        SUM(CASE WHEN journey_status = 14 THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_customer_id ORDER BY journey_created_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_driver_cancelled,

        SUM(CASE WHEN journey_status IN (15,19) THEN 1 ELSE 0 END)
            OVER (PARTITION BY ref_customer_id ORDER BY journey_created_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_admin_cancelled,

        COUNT(*) 
            OVER (PARTITION BY ref_customer_id ORDER BY journey_created_at
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_total_trips
    FROM prod_etl_data.tbl_journey_master
    WHERE 
       DATE((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <  '2025-11-21'
)

-- ============================================================
-- 2. ADD TIME SINCE LAST TRIP + FIRST TIME USER FLAG
-- ============================================================
,prev_features AS (
    SELECT
        o.*,

        CASE  WHEN prev_trip_ts IS NULL THEN NULL ELSE DATEDIFF(Day, prev_trip_ts, journey_ts) END AS time_since_last_trip_day,  

        CASE WHEN prev_promo IS NOT NULL THEN 1 ELSE 0 END AS promo_used_last_trip,
         CASE WHEN ref_promo_applied IS NOT NULL THEN 1 ELSE 0 END AS promo_applied_current_trip
    FROM ordered o
)

-- ============================================================
-- 3. TRIPS IN NEXT 30 MIN FOR THE SAME CUSTOMER
-- ============================================================
,trips_next_30min AS (
    SELECT
        w.*,
        (
            SELECT COUNT(*)
            FROM base b2
            WHERE b2.ref_customer_id = w.ref_customer_id
              AND b2.journey_ts > w.journey_ts
              AND b2.journey_ts <= w.journey_ts + INTERVAL '30 minute'
        ) AS trips_in_next_30min
    FROM prev_features w
)
-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from trips_next_30min;
-- 468168	468168	119530	

-- ============================================================
-- FINAL FEATURE TABLE FOR RETENTION MODEL
-- ============================================================
,model_features AS (
    SELECT
        t.*,

        -- Create retention label for model (already computed earlier)
        CASE WHEN EXISTS (
            SELECT 1 FROM future_trips ft
            WHERE ft.ref_customer_id = t.ref_customer_id
              AND ft.journey_ts > t.journey_ts
              AND ft.journey_ts <= t.journey_ts + INTERVAL '30 day'
        )
        THEN 1 ELSE 0 END AS retained_30d_label

    FROM trips_next_30min t
)
-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from trips_next_30min;
-- 468168	468168	119530	

-- select count(*), count(distinct journey_id), count(distinct ref_customer_id) from(
-- -------------------------------------------
--  FINAL MASTER TABLE (WITH ALL FEATURES)
-- -------------------------------------------
SELECT
    mf.journey_id,
    mf.ref_customer_id,
    mf.journey_ts,
    mf.journey_dt,
    mf.local_booking_hr,
    mf.weekday,
    CASE
          WHEN weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday') AND local_booking_hr IN (8,9) THEN 'Morning_peak'
          WHEN weekday IN ('Monday','Tuesday','Wednesday','Thursday') AND local_booking_hr IN (16,17,18,19) THEN 'Evening_weekday_peak'
          WHEN weekday IN ('Friday','Saturday','Sunday') AND local_booking_hr IN (16,17,18,19,20,21)   THEN 'Weekend_evening_peak'
		  WHEN weekday IN ('Friday','Saturday','Sunday') AND local_booking_hr IN (22,23)   THEN 'Weekend_mid_night'
          WHEN weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday') AND local_booking_hr IN (0,1,2,3,4,5)  THEN 'LateNight0_5'
          WHEN weekday NOT IN ('Friday','Saturday','Sunday') AND local_booking_hr IN (22,23)     THEN 'Weekday_mid_night'
          ELSE 'Others'
        END as shift,
    mf.Completed_trip,
    mf.customer_cancelled,
    mf.driver_cancelled,
    mf.admin_cancelled,

    prev_completed_trip,
    prev_customer_cancelled,
    prev_driver_cancelled,
    prev_admin_cancelled,
    -- Trip sequence features
    p.trip_seq_num,
    CASE WHEN p.trip_seq_num = 1 THEN 1 ELSE 0 END AS first_time_user_flag,
    p.prior_completed_trips,

    mf.time_since_last_trip_day,

    -- Promo & Payment
    mf.ref_promo_applied,
    promo_applied_current_trip,
    mf.prev_promo AS promo_used_last_trip,
    mf.payment_description AS payment_method,

    -- Fare / Pricing features
    mf.estimate_total_fee,
    mf.actual_total_fee,
    mf.price_error,
    mf.price_bucket,
    mf.fee_bucket,
    mf.drop_dist_m,
    mf.actual_distance,

    -- ETA / ATA
    mf.dispatch_eta,
    mf.ata_min,

    -- Location features
    mf.pickup_latitude,
    mf.pickup_longitude,
    mf.actual_drop_off_latitude,
    mf.actual_drop_off_longitude,
    mf.drop_hood,
    mf.drop_zone,

    -- Behavioural features
    mf.trips_in_next_30min,
    mf.vehicle_cat,


    -- Ratings / Complaints
    rt.rating,
    CASE WHEN rt.rating < 4 THEN 1 ELSE 0 END AS low_rating_lt4,
    CASE WHEN rt.rating < 3 THEN 1 ELSE 0 END AS low_rating_lt3,
    mf.complaint_24h,


    os.cum_completed_trips      AS overall_completed_trips,
    os.cum_customer_cancelled   AS overall_customer_cancelled_trips,
    os.cum_driver_cancelled     AS overall_driver_cancelled_trips,
    os.cum_admin_cancelled      AS overall_admin_cancelled_trips,
    os.cum_total_trips          AS overall_total_trips,

    os.cum_customer_cancelled::FLOAT / NULLIF(os.cum_total_trips,0) AS overall_customer_cancel_rate,
    os.cum_driver_cancelled::FLOAT   / NULLIF(os.cum_total_trips,0) AS overall_driver_cancel_rate,
    os.cum_admin_cancelled::FLOAT    / NULLIF(os.cum_total_trips,0) AS overall_admin_cancel_rate,
    os.cum_completed_trips::FLOAT    / NULLIF(os.cum_total_trips,0) AS overall_completion_rate,


    -- Target variable (Y)
    mf.retained_30d_label

FROM model_features mf
LEFT JOIN prior_trips p 
    ON p.journey_id = mf.journey_id
LEFT JOIN ratings rt 
    ON rt.ref_journey_id = mf.ref_journey_id

LEFT JOIN overall_status os
    ON os.journey_id = mf.journey_id;

-- 468168	468168	119530	
