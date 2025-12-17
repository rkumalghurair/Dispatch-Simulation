-- GRANT USAGE ON SCHEMA test_ric TO readonly_user;
-- GRANT SELECT ON TABLE test_ric.customer_retention_2 TO readonly_user;

create table test_ric.customer_retention_2 as 

-- drop table test_ric.customer_retention_2 
WITH users AS (
    SELECT 
        _id AS ref_customer_id,
        useruid AS customer_id,
        mobilenumber
    FROM public.users 
    WHERE usertype = 1
)
,
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
             ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')THEN 'Taxi' ELSE 'Limo' END AS vehicle_cat,

        (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
        DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS journey_dt,
        EXTRACT(HOUR FROM (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS local_booking_hr,
        TRIM(TO_CHAR((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai','Day')) AS weekday,
         case when  1=1 then 1 else 0 end  jrny_request,
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
      AND DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-03-01' 
      AND  DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <'2025-12-10'
)

-- select count(*), count(distinct customer_id), count(distinct journey_id )from base; -- 2,035,082,  --217927, --2035082


-- -------------------------------------------
--  MAP DROP LOCATION TO HOOD / ZONE
-- -------------------------------------------
,geo AS (
    SELECT 
        b.journey_id,
        h.name AS pickup_hood,
        h.zone_name AS pickup_zone
        
    FROM base b
    LEFT JOIN prod_etl_temp.tbl_hood_kml_with_zone h 
    ON ST_Contains(
        h.geom,
        ST_SetSRID(ST_Point(b.pickup_longitude, b.pickup_latitude), ST_SRID(h.geom))
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
        g.pickup_hood,
        g.pickup_zone,

        ST_DistanceSphere(
            ST_MakePoint(estimate_drop_off_longitude, estimate_drop_off_latitude),
            ST_MakePoint(actual_drop_off_longitude, actual_drop_off_latitude)
        ) AS drop_dist_m,

        (actual_total_fee - estimate_total_fee) / NULLIF(estimate_total_fee,0) AS price_error,

        CASE 
         when actual_total_fee is null then null
            WHEN actual_total_fee <= 30 THEN '0.<30'
            WHEN actual_total_fee <= 50 THEN '1.30-50'
            WHEN actual_total_fee <= 70 THEN '2.50-70'
            ELSE '3.>70'
        END AS fee_bucket,

        CASE when actual_total_fee is null then null 
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
-- select distinct payment_description from time_features; CARD, CASH
-- select count(*), count(distinct journey_id), count(distinct customer_id) from time_features;
-- FEATURES BUILDING


-- -------------------------------------------
--  CUSTOMER x WEEK ROLLING WINDOW FEATURES
-- -------------------------------------------

-- Weeks where the customer actually had journeys
,customer_weeks_core AS (
    SELECT DISTINCT 
        DATE_TRUNC('week', journey_dt)::date AS week_start,
        ref_customer_id,
        customer_id
    FROM time_features
)


-- One extra week AFTER last journey for each customer
,customer_lastweek AS (
    SELECT
        DATE_TRUNC('week', MAX(journey_dt) + INTERVAL '7 days')::date AS week_start,
        ref_customer_id,
        customer_id
    FROM time_features
    GROUP BY 2,3
)

-- Combine both
,customer_weeks AS (
    SELECT * FROM customer_weeks_core
    UNION
    SELECT * FROM customer_lastweek
)
-- select * from customer_weeks where customer_id='CUS_ZXFFV60310' order by week_start desc;


,weekly_customer_features AS (
    SELECT
        cw.week_start,
        cw.ref_customer_id,
        cw.customer_id,

        -- 14-day history window: [week_start - 14d, week_start)
        COUNT(tf.journey_id) AS total_journeys_14d,

        SUM(CASE WHEN tf.Completed_trip = 1 THEN 1 ELSE 0 END)          AS completed_journeys_14d,
        SUM(CASE WHEN tf.customer_cancelled = 1 THEN 1 ELSE 0 END)      AS customer_cancelled_14d,
        SUM(CASE WHEN tf.driver_cancelled = 1 THEN 1 ELSE 0 END)        AS driver_cancelled_14d,
        SUM(CASE WHEN tf.admin_cancelled = 1 THEN 1 ELSE 0 END)         AS admin_cancelled_14d,

        SUM(CASE WHEN tf.vehicle_cat = 'Taxi' THEN 1 ELSE 0 END)        AS taxi_rides_14d,
        SUM(CASE WHEN tf.vehicle_cat = 'Limo' THEN 1 ELSE 0 END)        AS limo_rides_14d,

        SUM(
            CASE  WHEN tf.weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday') AND tf.local_booking_hr BETWEEN 15 AND 19 THEN 1 ELSE 0 
            END) AS evening_peak_rides_14d,   -- Mon–Fri, 3–8pm

        SUM(
            CASE WHEN tf.weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday') AND  tf.local_booking_hr BETWEEN 7 AND 9 THEN 1 ELSE 0 END
        ) AS morning_peak_rides_14d,   -- 7–10am, all days

        SUM(CASE  WHEN tf.weekday IN ('Saturday','Sunday') THEN 1 ELSE 0  END ) AS weekend_rides_14d,

        SUM( CASE  WHEN tf.weekday NOT IN ('Saturday','Sunday') THEN 1 ELSE 0  END ) AS weekday_rides_14d,

        AVG(NULLIF(tf.dispatch_eta,0))                               AS avg_eta_14d,
        -- PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY tf.dispatch_eta) AS p70_eta_14d,
        -- PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY tf.dispatch_eta) AS p90_eta_14d,

        AVG(tf.estimate_total_fee)                                   AS avg_estimated_fare_14d,
        AVG(tf.actual_distance)                                      AS avg_distance_14d,

        AVG(CASE  WHEN tf.ref_promo_applied IS NOT NULL THEN 1.0  ELSE 0.0 END ) AS share_trips_with_promo_14d,

        AVG(CASE WHEN tf.payment_description  NOT LIKE '%CASH%' THEN 1.0 ELSE 0.0 END) AS share_trips_card_14d,

        AVG( CASE  WHEN tf.payment_description ILIKE '%CASH%' THEN 1.0  ELSE 0.0  END ) AS share_trips_cash_14d,

        AVG(CASE  WHEN ABS(tf.actual_total_fee - tf.estimate_total_fee) > 5 AND tf.Completed_trip = 1  THEN 1.0 ELSE 0.0  END ) AS share_trips_fare_diff_gt_5_14d,

        AVG( CASE WHEN ABS(tf.actual_total_fee - tf.estimate_total_fee) > 10  AND tf.Completed_trip = 1 THEN 1.0  ELSE 0.0  END ) AS share_trips_fare_diff_gt_10_14d,

        AVG(CASE WHEN ABS(tf.actual_total_fee - tf.estimate_total_fee) > 15 AND tf.Completed_trip = 1  THEN 1.0 ELSE 0.0 END ) AS share_trips_fare_diff_gt_15_14d,

        AVG(tf.ata_min)                                              AS avg_ata_14d,
        -- PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tf.ata_min)      AS median_ata_14d,
        -- PERCENTILE_CONT(0.7) WITHIN GROUP (ORDER BY tf.ata_min)      AS p70_ata_14d,


        AVG( CASE  WHEN tf.ata_min <= 3 AND tf.Completed_trip = 1 THEN 1.0 ELSE 0.0  END) AS share_trips_ata_lt_3_14d ,
        AVG( CASE  WHEN tf.ata_min > 3  AND  tf.Completed_trip = 1 THEN 1.0 ELSE 0.0  END) AS share_trips_ata_gt_3_14d ,
  
        AVG( CASE  WHEN tf.dispatch_eta <= 3 THEN 1.0 ELSE 0.0  END) AS share_trips_etta_lt_3_14d ,
        AVG( CASE  WHEN tf.dispatch_eta > 3 THEN 1.0 ELSE 0.0  END) AS share_trips_eta_gt_3_14d ,

        AVG( CASE  WHEN (tf.ata_min - tf.dispatch_eta) > 2 AND tf.Completed_trip = 1 THEN 1.0  ELSE 0.0  END) AS share_trips_ata_gt_eta_by_2m_14d,

        -- lifetime prior requests before this week_start (all-time history)
        COALESCE(
            (
                SELECT COUNT(*)
                FROM time_features tf_life
                WHERE tf_life.ref_customer_id = cw.ref_customer_id
                  AND tf_life.journey_dt < cw.week_start
            ),
            0
        ) AS prior_requests_all_time

    FROM customer_weeks cw
    LEFT JOIN time_features tf
      ON tf.ref_customer_id = cw.ref_customer_id
     AND tf.journey_dt >= cw.week_start - INTERVAL '14 days'
     AND tf.journey_dt <  cw.week_start
    GROUP BY
        cw.week_start,
        cw.ref_customer_id,
        cw.customer_id
)

-- select * from weekly_customer_features;
-- 2025-03-10	67b2f01cff3121dbd8e0d15e	CUS_BDVSB23442	10	7	3	0	0	10	0	2	0	6	4	2.9	25.3	6.059428571428571	0.6	1	0	0.3	0.3	0.3	2	0.7	0.1	10	


-- -------------------------------------------
--  CHURN LABEL: retained in next 30 days (Y)
-- -------------------------------------------
,churn_label AS (
    SELECT
        w.week_start,
        w.ref_customer_id,
        CASE  WHEN COUNT(tf_future.journey_id) > 0 THEN 1  ELSE 0  END AS retained_next_30d
    FROM weekly_customer_features w
    LEFT JOIN time_features tf_future
      ON tf_future.ref_customer_id = w.ref_customer_id
     AND tf_future.journey_dt >  w.week_start
     AND tf_future.journey_dt <= w.week_start + INTERVAL '30 days'
    --  AND tf_future.jrny_request = 1
    GROUP BY 1,2
)
-- -------------------------------------------
--  FINAL TRAINING TABLE (EXISTING USERS ONLY)
-- -------------------------------------------
SELECT
    w.week_start,
    w.ref_customer_id,
    w.customer_id,
    w.total_journeys_14d,
    w.completed_journeys_14d,
    w.customer_cancelled_14d,
    w.driver_cancelled_14d,
    w.admin_cancelled_14d,
    w.taxi_rides_14d,
    w.limo_rides_14d,
    w.evening_peak_rides_14d,
    w.morning_peak_rides_14d,
    w.weekend_rides_14d,
    w.weekday_rides_14d,
    w.avg_eta_14d,

    w.avg_estimated_fare_14d,
    w.avg_distance_14d,
    w.share_trips_with_promo_14d,
    w.share_trips_card_14d,
    w.share_trips_cash_14d,
    w.share_trips_fare_diff_gt_5_14d,
    w.share_trips_fare_diff_gt_10_14d,
    w.share_trips_fare_diff_gt_15_14d,

    w.avg_ata_14d,
    -- w.median_ata_14d,
    w.share_trips_ata_lt_3_14d,
    w.share_trips_ata_gt_3_14d,
    w.share_trips_etta_lt_3_14d,
    w.share_trips_eta_gt_3_14d,

    w.share_trips_ata_gt_eta_by_2m_14d,
    w.prior_requests_all_time,
    c.retained_next_30d          AS y_retained_next_30d

FROM weekly_customer_features w
JOIN churn_label c
  ON c.ref_customer_id = w.ref_customer_id
 AND c.week_start      = w.week_start ;
-- keep only "existing" users: more than 1 lifetime journey
-- WHERE w.prior_requests_all_time > 1;


select * from test_ric.customer_retention_2 limit 10
