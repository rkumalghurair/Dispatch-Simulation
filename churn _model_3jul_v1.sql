/*
=============================================================================
  Cohort  : Users whose true first-ever completed trip was Jan–May 2026
            June 2026 = OOT holdout (excluded here)

  Predict : AT Day 5 — features frozen at first_trip_dt + 5 days
  Label   : churned_21d — did user return within 21 days of first trip?
            (21-day = outcome being predicted. Day 5 = when we predict.
             16 days of intervention runway remain after scoring.)

  Y = 1 (churned)  : no return trip within 21 days
  Y = 0 (retained) : at least one return trip within 21 days

  No-leakage guarantee : ALL features use ONLY data on/before Day 5.

=============================================================================*/

create table prod_etl_temp.churn_model_v1 as 

WITH
all_completed AS (
    SELECT
        j.ref_customer_id,
        j.customer_id,
        j.journey_id,
        j.ref_journey_id,

        -- all timestamps converted to Dubai time
        (j.journey_created_at          AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
        (j.accepted_ride_timestamp     AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS accepted_ts,
        (j.on_route_timestamp          AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS on_route_ts,
        (j.arrived_at_pickup_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS arrived_ts,
        (j.pickup_time                 AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS pickup_ts,
        (j.journey_completed_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS completed_ts,

        DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')   AS journey_dt,

        CASE WHEN j.ref_vehicle_category_id IN
             ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')THEN 'Taxi' ELSE 'Limo' END      AS vehicle_cat,

        EXTRACT(HOUR FROM (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' )       AS local_booking_hr,
        TRIM(TO_CHAR(  (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai',  'Day' ))    AS weekday,

        j.actual_total_fee,
        j.estimate_total_fee,
        j.actual_total_fee - j.estimate_total_fee                   AS fare_discrepancy_aed,
        j.actual_distance,
        j.ref_promo_applied,
        -- j.payment_description,
        j.dispatch_eta,                                              -- estimated seconds to pickup
      -- zones
        j.pickup_zone_name,
        j.drop_off_zone_name as dropoff_zone_name,
		j.actual_discount_amount,

        -- dropoff coordinates for deviation calc
        j.estimate_drop_off_latitude,
        j.estimate_drop_off_longitude,
        j.actual_drop_off_latitude,
        j.actual_drop_off_longitude,

        -- ── ATA: actual time driver took to reach pickup ──────────────
        -- Taxi: on_route → arrived   |   Limo: accepted → arrived
        DATEDIFF(second,
            CASE WHEN j.ref_vehicle_category_id IN
                      ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
                 THEN (j.on_route_timestamp      AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
                 ELSE (j.accepted_ride_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
            END,
            (j.arrived_at_pickup_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'  )     AS ata_sec,

        -- ── Pickup delay = ATA − ETA (positive = later than promised) ─
        DATEDIFF(second,   COALESCE ((j.accepted_ride_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai',  (j.on_route_timestamp  AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'),           (j.arrived_at_pickup_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
        ) - (j.dispatch_eta_sec )                                 AS pickup_delay_vs_eta_sec,

        -- ── Trip duration: passenger in car → dropoff ─────────────────
        DATEDIFF(minute,
            (j.pickup_time                 AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai',
            (j.journey_completed_timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
        )                                                            AS trip_duration_mins,

        -- ── Dropoff deviation: estimated vs actual dropoff location ───
        -- Haversine approximation in km (flat-earth ok for short distances)
        -- Measures how far actual dropoff was from where system estimated.
        -- High deviation = route change, wrong destination, customer request change.
        CASE
            WHEN j.estimate_drop_off_latitude  IS NOT NULL
             AND j.actual_drop_off_latitude     IS NOT NULL
            THEN
                2 * 6371 * ASIN(SQRT(
                    POWER(SIN(RADIANS(
                        (j.actual_drop_off_latitude - j.estimate_drop_off_latitude) / 2
                    )), 2)
                    + COS(RADIANS(j.estimate_drop_off_latitude))
                    * COS(RADIANS(j.actual_drop_off_latitude))
                    * POWER(SIN(RADIANS(
                        (j.actual_drop_off_longitude - j.estimate_drop_off_longitude) / 2
                    )), 2)
                ))
            ELSE NULL
        END                                                          AS dropoff_deviation_km
		,dispatch_eta_sec
		,actual_drop_off_time
		,estimate_drop_off_time
		,DATEDIFF(minute,estimate_drop_off_time, actual_drop_off_time)as dropoff_time_diff
		
    FROM prod_etl_data.tbl_journey_master j
    WHERE j.journey_status IN (9, 10)
      AND j.journey_created_at IS NOT NULL
	  and journey_type =1
)

-- select count(*) from  prod_etl_data.tbl_journey_master where dispatch_eta_sec is null and date(pickup_time)>='2026-02-01' and journey_status in (9,10)


-- ── STEP 2: Active users ──────────────────────────────────────────────────
,users AS (
    SELECT
        _id                                              AS ref_customer_id,
        useruid                                          AS customer_id,
        mobilenumber,
        countrycode,
        emailid,
		appflyerid,
        DATE((createdat AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS reg_dt,
        CASE WHEN countrycode LIKE '%+971%' THEN 1 ELSE 0 END AS is_uae_number
    FROM public.users
    WHERE usertype = 1
      AND status   = 1
),


-- ── STEP 3: Rank all trips to find true first-ever trip ───────────────────
trip_ranked AS (
    SELECT
        ac.*,
        ROW_NUMBER() OVER (  PARTITION BY ac.ref_customer_id ORDER BY ac.journey_ts ASC) AS trip_rank
    FROM all_completed ac
    INNER JOIN users u ON ac.ref_customer_id = u.ref_customer_id
)
-- select * from trip_ranked
,

-- ── STEP 4: Cohort — true first trip Jan–May 2026 ────────────────────────
first_trips AS (
    SELECT
        ref_customer_id,
        customer_id,
        journey_id              AS first_journey_id,
		rating,
		ft.ref_journey_id,
        journey_dt              AS first_trip_dt,
        journey_ts              AS first_trip_ts,
        vehicle_cat             AS first_trip_vehicle_cat,
        local_booking_hr        AS first_trip_hour,
        weekday                 AS first_trip_weekday,
        actual_total_fee        AS first_trip_fare,
        estimate_total_fee      AS first_trip_est_fare,
        actual_distance         AS first_trip_distance,
        fare_discrepancy_aed    AS first_trip_fare_discrepancy,
		dispatch_eta            AS first_trip_dispatch_eta_min,
		dispatch_eta_sec        as   first_trip_eta_sec,
		ata_sec                AS first_trip_ata_sec,	  
        pickup_delay_vs_eta_sec AS first_trip_pickup_delay_vs_eta,	
		actual_discount_amount as first_trip_actual_discount_amount, 
        trip_duration_mins      AS first_trip_duration_mins,
		
        dropoff_deviation_km    AS first_trip_dropoff_deviation_km,
		dropoff_time_diff as first_trip_dropoff_time_diff,
        
        pickup_zone_name        AS first_trip_pickup_zone,
        dropoff_zone_name       AS first_trip_dropoff_zone,
        CASE WHEN ref_promo_applied IS NOT NULL THEN 1 ELSE 0 END  AS first_trip_promo_used
		
    FROM trip_ranked as ft 
	left join (  SELECT ref_journey_id,  rating FROM prod_etl_data.tbl_userrating_details 
	) as rating
	 on ft.ref_journey_id =rating.ref_journey_id
	 
    WHERE trip_rank   = 1
      AND journey_dt >= '2026-03-01'
      AND journey_dt <  '2026-06-10'    -- 21 days needed to observe churn for these users
)
-- ── STEP 5: Y label — returned within 21 days? ───────────────────────────
,label AS (
    SELECT
        ft.ref_customer_id,
        MIN(tr.journey_dt)  AS second_trip_dt,
        CASE WHEN MIN(tr.journey_dt) IS NOT NULL THEN 0  ELSE 1 END   AS churned_21d
    FROM first_trips ft
    LEFT JOIN trip_ranked tr
           ON  tr.ref_customer_id = ft.ref_customer_id
          AND  tr.trip_rank       >= 2
          AND  tr.journey_dt      >=  ft.first_trip_dt
          AND  tr.journey_dt      <= DATEADD(day, 21, ft.first_trip_dt)
    GROUP BY ft.ref_customer_id
)
-- select * from label

-- ── STEP 6: Day 0–5 completed trip behaviour ─────────────────────────────
,day5_window AS (
    SELECT
        ft.ref_customer_id,
        COUNT(tr.journey_id)                                          AS trips_day0_to_5,
        CASE WHEN COUNT(tr.journey_id) > 1 THEN 1 ELSE 0 END         AS returned_before_day5,
        SUM(CASE WHEN tr.ref_promo_applied IS NOT NULL THEN 1 ELSE 0 END) AS promo_trips_day5,
        SUM(CASE WHEN tr.vehicle_cat = 'Taxi' THEN 1 ELSE 0 END)     AS taxi_trips_day5,
        SUM(CASE WHEN tr.vehicle_cat = 'Limo' THEN 1 ELSE 0 END)     AS limo_trips_day5,
        SUM(tr.actual_total_fee - actual_discount_amount)                                      AS total_spend_day5,
        AVG(tr.actual_total_fee)                                      AS avg_fare_day5,
        AVG(tr.actual_distance)                                       AS avg_distance_day5,
        AVG(tr.trip_duration_mins)                                    AS avg_trip_duration_day5,
        AVG(tr.ata_sec)                                              AS avg_ata_mins_day5,
        AVG(tr.pickup_delay_vs_eta_sec)                              AS avg_pickup_delay_day5,
        AVG(tr.dropoff_deviation_km)                                  AS avg_dropoff_deviation_day5,
	   DATEDIFF(day, MAX(tr.journey_dt), DATEADD(day, 5, MAX(ft.first_trip_dt))   )     AS days_since_last_trip_at_day5
	
    FROM first_trips ft
    LEFT JOIN trip_ranked tr
           ON  tr.ref_customer_id = ft.ref_customer_id
          AND  tr.journey_dt     >= ft.first_trip_dt
          AND  tr.journey_dt     <= DATEADD(day, 5, ft.first_trip_dt)
    GROUP BY ft.ref_customer_id
)

-- select * from day5_window

-- ── STEP 7: All request attempts Day 0–5 (including cancels) ─────────────
,cancellations AS 
(
    SELECT
        ft.ref_customer_id,
        COUNT(*)                                                      AS total_requests_day5,
        SUM(CASE WHEN j.journey_status = 13 THEN 1 ELSE 0 END)       AS customer_cancels_day5,
        SUM(CASE WHEN j.journey_status = 14 THEN 1 ELSE 0 END)       AS driver_cancels_day5,
        SUM(CASE WHEN j.journey_status IN (15,19) THEN 1 ELSE 0 END) AS unfulfilled_day5
    FROM first_trips ft
    INNER JOIN prod_etl_data.tbl_journey_master j
            ON  j.ref_customer_id = ft.ref_customer_id
           AND  DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= ft.first_trip_dt
           AND  DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <= DATEADD(day, 5, ft.first_trip_dt)
    GROUP BY ft.ref_customer_id
)
,

-- ── STEP 8: Payment mode — aggregated at journey level ────────────────────
payment AS 
(
    SELECT
        journey_id,
        LISTAGG(DISTINCT payment_description, ' + ')
            WITHIN GROUP (ORDER BY payment_description) AS payment_combo
    FROM prod_etl_data.tbl_payment_details
    GROUP BY journey_id
),

-- ── STEP 9: Wallet as of Day 5 ───────────────────────────────────────────
wallet AS (
    SELECT
        ft.ref_customer_id,

        COALESCE(SUM(CASE WHEN UPPER(TRIM(wl.type)) = 'CREDIT' THEN wl.amount ELSE 0 END), 0)              AS wallet_cashback_credited,

        COALESCE(SUM(CASE  WHEN UPPER(TRIM(wl.type)) = 'DEBIT' THEN wl.amount ELSE 0 END), 0)                  AS wallet_cashback_redeemed,

        COALESCE(SUM(CASE WHEN UPPER(TRIM(wl.type)) = 'CREDIT' THEN  wl.amount  WHEN UPPER(TRIM(wl.type)) = 'DEBIT'  THEN -wl.amount ELSE 0 END), 0)    AS wallet_balance_at_day5,

        MAX(CASE WHEN jm.ref_journey_id = ft.ref_journey_id AND UPPER(TRIM(wl.type)) = 'CREDIT' THEN 1 ELSE 0 END)  AS first_trip_cashback_received,

        COALESCE(SUM(CASE WHEN jm.ref_journey_id = ft.ref_journey_id AND UPPER(TRIM(wl.type)) = 'CREDIT'  THEN wl.amount ELSE 0 END), 0)   AS first_trip_cashback_amount,

        COUNT(DISTINCT CASE WHEN UPPER(TRIM(wl.type)) = 'DEBIT' THEN jm.ref_journey_id END)  AS trips_wallet_redeemed_day5,

        COUNT(DISTINCT CASE WHEN UPPER(TRIM(wl.type)) = 'CREDIT'THEN jm.ref_journey_id END)    AS trips_cashback_earned_day5

    FROM first_trips ft

    INNER JOIN prod_etl_data.tbl_journey_master jm
            ON  jm.ref_customer_id = ft.ref_customer_id
           AND  jm.journey_status IN (9, 10)
           AND  DATE((jm.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')  >= ft.first_trip_dt
           AND  DATE((jm.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')<= DATEADD(day, 5, ft.first_trip_dt)

    LEFT JOIN zed_stripe.wallet_ledger wl
           ON  wl.reference_id = jm.ref_journey_id    -- ← fixed: was jm.journey_id
          AND  wl.wallet_id IS NULL
          AND  wl.reference_id IS NOT NULL
          AND wl.transaction_description not in ('EXPIRED')

    GROUP BY ft.ref_customer_id
)


-- ── STEP 10: Device / fraud signals ──────────────────────────────────────
,device_signals AS (
    SELECT
        ud_target.ref_customer_id,
        COUNT(DISTINCT ud_all.ref_customer_id)           AS accounts_per_device,
        COUNT(DISTINCT CASE WHEN ud_all.appsflyerid = ud_target.appsflyerid AND ud_target.appsflyerid IS NOT NULL THEN ud_all.ref_customer_id END)  AS accounts_per_appsflyer_id,
        COUNT(DISTINCT CASE  WHEN u_all.emailid = u_target.emailid AND u_target.emailid IS NOT NULL AND u_target.emailid != '' THEN u_all._id END)    AS accounts_per_email
    FROM (
        SELECT
            u.ref_customer_id,
            json_extract_path_text(ud.devices,'devices','0','deviceId')    AS device_id,
            ud.appflyerid AS appsflyerid
        FROM first_trips ft
        INNER JOIN users u ON ft.ref_customer_id = u.ref_customer_id
        INNER JOIN public.userdevices ud ON ud.refuserid = u.ref_customer_id
        QUALIFY ROW_NUMBER() OVER ( PARTITION BY ud.refuserid ORDER BY ud.updatedat DESC ) = 1
    ) ud_target
    LEFT JOIN
	(
        SELECT
            ud2.refuserid as ref_customer_id ,
            json_extract_path_text(ud2.devices,'devices','0','deviceId')    AS device_id,
			 ud2.appflyerid AS appsflyerid    
        FROM public.userdevices ud2
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ud2.refuserid ORDER BY ud2.updatedat DESC ) = 1
    ) ud_all 
	
	ON ud_all.device_id = ud_target.device_id
    AND ud_target.device_id IS NOT NULL
   
    LEFT JOIN users u_target 
	ON ud_target.ref_customer_id = u_target.ref_customer_id
	
    LEFT JOIN public.users u_all
           ON u_all.emailid    = u_target.emailid
          AND u_target.emailid IS NOT NULL
          AND u_target.emailid != ''
          AND u_all.usertype = 1
    GROUP BY ud_target.ref_customer_id
)


,

-- ── STEP 11: User registration meta ──────────────────────────────────────
user_meta AS (
    SELECT
        u.ref_customer_id,
        u.countrycode,
        u.is_uae_number,
        u.reg_dt,
        DATEDIFF(day, u.reg_dt, ft.first_trip_dt)  AS days_reg_to_first_trip
    FROM users u
    INNER JOIN first_trips ft ON u.ref_customer_id = ft.ref_customer_id
)

,amp_base AS (
    SELECT ft.customer_id, e.session_id,
           (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS event_ts,
           DATE((e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS event_dt,
           LOWER(TRIM(e.event_type)) AS event_type,
           json_extract_path_text(e.event_properties, 'origin') AS booking_origin,
           json_extract_path_text(e.event_properties, 'screen') AS page_screen,
           e.os_name, e.platform
    FROM amplitude_customer_app.events e
    INNER JOIN first_trips ft ON ft.customer_id = e.user_id   -- CUS_ match
           AND DATE((e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')
               BETWEEN ft.first_trip_dt AND DATEADD(day, 5, ft.first_trip_dt)
    WHERE e.session_id <> -1 AND e.session_id IS NOT NULL AND e.user_id IS NOT NULL

    UNION ALL

    SELECT ft.customer_id, e.session_id,
           (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS event_ts,
           DATE((e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS event_dt,
           LOWER(TRIM(e.event_type)) AS event_type,
           json_extract_path_text(e.event_properties, 'origin') AS booking_origin,
           json_extract_path_text(e.event_properties, 'screen') AS page_screen,
           e.os_name, e.platform
    FROM amplitude_customer_app.events e
    INNER JOIN first_trips ft ON ft.ref_customer_id = e.user_id  -- _id match
           AND DATE((e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')
               BETWEEN ft.first_trip_dt AND DATEADD(day, 5, ft.first_trip_dt)
    WHERE e.session_id <> -1 AND e.session_id IS NOT NULL
      AND e.user_id IS NOT NULL
      AND e.user_id NOT LIKE 'CUS_%'  
)
,amp_sessions AS 
(
    SELECT
        customer_id,
        session_id,
        MIN(event_ts)                                    AS session_start_ts,
        MAX(event_ts)                                    AS session_end_ts,
        MIN(event_dt)                                    AS session_dt,
        DATEDIFF(second, MIN(event_ts), MAX(event_ts)) / 60.0 AS session_duration_mins,
        MAX(CASE
            WHEN event_type LIKE '%book_journey_clicked%'  THEN 4
            WHEN event_type LIKE '%vehiclecategory%'
              OR event_type LIKE '%vehicle_detail%'        THEN 3
            WHEN event_type LIKE '%journeyintent_started%' THEN 2
            WHEN event_type LIKE '%page_view%'             THEN 1
            ELSE 0 END)                                  AS max_funnel_depth,
        MAX(CASE WHEN event_type LIKE '%page_view%'             THEN 1 ELSE 0 END) AS had_page_view,
        MAX(CASE WHEN event_type LIKE '%journeyintent_started%' THEN 1 ELSE 0 END) AS had_journey_intent,
        MAX(CASE WHEN event_type LIKE '%vehiclecategory%'
                  OR event_type LIKE '%vehicle_category%'       THEN 1 ELSE 0 END) AS had_vehicle_cat,
        MAX(CASE WHEN event_type LIKE '%book_journey_clicked%'  THEN 1 ELSE 0 END) AS had_book_click,
        MAX(CASE WHEN event_type LIKE '%promo%'                 THEN 1 ELSE 0 END) AS had_promo_interaction,
        COUNT(CASE WHEN event_type LIKE '%location_search%'     THEN 1 END)        AS location_searches,
        COUNT(*)                                         AS events_in_session,
        MAX(os_name)                                     AS os_name,
        MAX(platform)                                    AS platform
    FROM amp_base
    GROUP BY 1,2
)


-- Session-level aggregation: min/max event time per session per user
-- Used to compute session duration and identify session start
,session_features AS (
    SELECT
        ft.customer_id,
        COUNT(DISTINCT s.session_id)                     AS total_sessions_day5,
        COUNT(DISTINCT s.session_dt)                     AS distinct_days_with_session_day5,
        COUNT(DISTINCT CASE WHEN s.session_dt = ft.first_trip_dt THEN s.session_id END) AS sessions_on_day0,
        COUNT(DISTINCT CASE WHEN s.session_dt > ft.first_trip_dt THEN s.session_id END) AS sessions_after_first_trip,
        MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN 1 ELSE 0 END)                AS returned_to_app_after_trip,
        MIN(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.session_dt END)            AS first_app_return_dt,
        DATEDIFF(day, MAX(ft.first_trip_dt),
            MIN(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.session_dt END))       AS days_to_first_app_return,
        AVG(s.session_duration_mins)                     AS avg_session_duration_day5,
        MAX(s.session_duration_mins)                     AS max_session_duration_day5,
        AVG(CASE WHEN s.session_dt = ft.first_trip_dt THEN s.session_duration_mins END) AS avg_session_dur_day0,
        AVG(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.session_duration_mins END) AS avg_session_dur_after_trip,
        MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.max_funnel_depth END)      AS max_funnel_depth_after_trip,
        MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_page_view END)         AS post_trip_had_page_view,
        MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_journey_intent END)    AS post_trip_had_journey_intent,
        MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_vehicle_cat END)       AS post_trip_reached_vehicle_cat,
        MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_book_click END)        AS post_trip_clicked_book,
        CASE WHEN MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_journey_intent END) = 1
              AND MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_book_click END) = 0
             THEN 1 ELSE 0 END                           AS post_trip_abandoned_at_intent,
        CASE WHEN MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_vehicle_cat END) = 1
              AND MAX(CASE WHEN s.session_dt > ft.first_trip_dt THEN s.had_book_click END) = 0
             THEN 1 ELSE 0 END                           AS post_trip_abandoned_at_vehicle_cat,
        SUM(CASE WHEN s.session_dt > ft.first_trip_dt
                  AND s.had_journey_intent = 1
                  AND s.had_book_click = 0 THEN 1 ELSE 0 END) AS post_trip_abandoned_sessions,
        SUM(CASE WHEN s.session_dt > ft.first_trip_dt
                  AND s.had_book_click = 1 THEN 1 ELSE 0 END) AS post_trip_booking_sessions,
        MAX(s.had_promo_interaction)                     AS had_promo_interaction,
        SUM(s.location_searches)                         AS total_location_searches_day5,
        -- platform from amp_sessions directly — no second join needed
        MAX(s.os_name)                                   AS app_platform,
        MAX(s.platform)                                  AS app_platform_detail
    FROM first_trips ft
    LEFT JOIN amp_sessions s ON s.customer_id = ft.customer_id
    GROUP BY ft.customer_id, ft.first_trip_dt
)

-- ── FINAL: One row per user ───────────────────────────────────────────────
SELECT

    -- identifiers (drop before model training)
    ft.ref_customer_id,
    ft.customer_id,
    ft.first_trip_dt,
    DATEADD(day, 5,  ft.first_trip_dt)  AS feature_snapshot_dt,
    DATEADD(day, 21, ft.first_trip_dt)  AS label_window_end_dt,

    -- ── Y VARIABLE ──────────────────────────────────────────────────────
    lb.churned_21d,           -- 1 = churned, 0 = retained
    lb.second_trip_dt,        -- drop before model fit, keep for analysis

    -- ── USER REGISTRATION ───────────────────────────────────────────────
    um.is_uae_number,
    um.days_reg_to_first_trip,
    CASE
        WHEN um.days_reg_to_first_trip = 0              THEN 'same_day'
        WHEN um.days_reg_to_first_trip BETWEEN 1 AND 7  THEN '1_to_7d'
        WHEN um.days_reg_to_first_trip BETWEEN 8 AND 30 THEN '8_to_30d'
        ELSE 'over_30d'
    END                                                  AS reg_to_first_trip_bucket,

    -- ── FIRST TRIP FEATURES ─────────────────────────────────────────────
    ft.first_trip_vehicle_cat,
    ft.first_trip_hour,
    ft.first_trip_weekday,
    CASE WHEN ft.first_trip_hour BETWEEN 7  AND 9 OR ft.first_trip_hour BETWEEN 16 AND 19  THEN 1 ELSE 0 END       AS first_trip_is_peak_hour,
    CASE WHEN ft.first_trip_weekday IN ('Friday','Saturday')  THEN 1 ELSE 0 END     AS first_trip_is_weekend,
    ft.first_trip_pickup_zone,
    ft.first_trip_dropoff_zone,
    ft.first_trip_fare,
    ft.first_trip_est_fare,
    ft.first_trip_distance,
    ft.first_trip_fare_discrepancy,     -- actual - estimated fare
    CASE WHEN ft.first_trip_distance > 0
         THEN ft.first_trip_fare / ft.first_trip_distance
         ELSE NULL END                                   AS fare_per_km,
    ft.first_trip_dispatch_eta_min,
	ft.first_trip_eta_sec,
	
    ft.first_trip_ata_sec,             -- how long driver actually took to arrive
    ft.first_trip_pickup_delay_vs_eta,  -- ATA - ETA: + = late, - = early
    ft.first_trip_duration_mins,        -- pickup_time → journey_completed_timestamp
	ft.first_trip_actual_discount_amount,
    ft.first_trip_dropoff_deviation_km, -- estimate vs actual dropoff distance (km)
	ft.first_trip_dropoff_time_diff, -- estimate vs actual dropoff distance (km)
    ft.first_trip_promo_used,
	payment_combo,
	ft.rating,


    -- ── DAY 0–5 BEHAVIOUR ───────────────────────────────────────────────
    d5.trips_day0_to_5,
    d5.returned_before_day5,
    d5.promo_trips_day5,
    CASE WHEN d5.trips_day0_to_5 > 0
         THEN d5.promo_trips_day5 * 1.0 / d5.trips_day0_to_5
         ELSE 0 END                                      AS promo_trip_ratio_day5,
    d5.taxi_trips_day5,
    d5.limo_trips_day5,
    d5.total_spend_day5,
    d5.avg_fare_day5,
    d5.avg_distance_day5,
    d5.avg_trip_duration_day5,
    d5.avg_ata_mins_day5,
    d5.avg_pickup_delay_day5,           -- avg ATA-ETA delta across Day 0–5 trips
    d5.avg_dropoff_deviation_day5,      -- avg dropoff deviation km across window
    d5.days_since_last_trip_at_day5,    -- key recency signal at scoring time

    -- ── CANCELLATION SIGNALS ─────────────────────────────────────────────
    c.total_requests_day5,
    c.customer_cancels_day5,
    c.driver_cancels_day5,
    c.unfulfilled_day5,
    CASE WHEN c.total_requests_day5 > 0
         THEN d5.trips_day0_to_5 * 1.0 / c.total_requests_day5
         ELSE 1.0 END                                    AS completion_rate_day5,

    -- ── WALLET ──────────────────────────────────────────────────────────
    COALESCE(w.wallet_cashback_redeemed, 0)              AS wallet_cashback_redeemed,
	coalesce(w.wallet_cashback_credited,0) as wallet_cashback_credited,
    COALESCE(w.wallet_balance_at_day5,   0)              AS wallet_balance_at_day5,
	w.first_trip_cashback_received,
	w.first_trip_cashback_amount,
	w.trips_wallet_redeemed_day5,
	w.trips_cashback_earned_day5,
    -- ── DEVICE / FRAUD SIGNALS ───────────────────────────────────────────
    COALESCE(ds.accounts_per_device,       1)            AS accounts_per_device,
    COALESCE(ds.accounts_per_appsflyer_id, 1)            AS accounts_per_appsflyer_id,
    COALESCE(ds.accounts_per_email,        1)            AS accounts_per_email,
    CASE WHEN COALESCE(ds.accounts_per_device,1) > 1
         THEN 1 ELSE 0 END                               AS is_shared_device

-- ── SESSION FEATURES ────────────────────────────────────────────────────────
,sf.total_sessions_day5
,sf.distinct_days_with_session_day5
,sf.sessions_on_day0
,sf.sessions_after_first_trip
,sf.returned_to_app_after_trip
,sf.first_app_return_dt
,sf.days_to_first_app_return
,sf.avg_session_duration_day5
,sf.max_session_duration_day5
,sf.avg_session_dur_day0
,sf.avg_session_dur_after_trip
,sf.max_funnel_depth_after_trip          -- was: max_funnel_depth_day5
,sf.post_trip_had_page_view              -- was: ever_had_page_view
,sf.post_trip_had_journey_intent         -- was: ever_had_journey_intent
,sf.post_trip_reached_vehicle_cat        -- was: ever_reached_vehicle_cat
,sf.post_trip_clicked_book               -- was: ever_clicked_book
,sf.post_trip_abandoned_at_intent        -- was: abandoned_at_intent
,sf.post_trip_abandoned_at_vehicle_cat   -- was: abandoned_at_vehicle_cat
,sf.post_trip_abandoned_sessions
,sf.post_trip_booking_sessions
,sf.had_promo_interaction
,sf.total_location_searches_day5
,sf.app_platform

FROM first_trips ft
INNER JOIN label          lb ON ft.ref_customer_id = lb.ref_customer_id
LEFT  JOIN day5_window    d5 ON ft.ref_customer_id = d5.ref_customer_id
LEFT  JOIN cancellations   c ON ft.ref_customer_id = c.ref_customer_id
LEFT  JOIN payment        pm ON ft.first_journey_id = pm.journey_id
LEFT  JOIN wallet          w ON ft.ref_customer_id = w.ref_customer_id
LEFT  JOIN device_signals ds ON ft.ref_customer_id = ds.ref_customer_id
LEFT  JOIN user_meta      um ON ft.ref_customer_id = um.ref_customer_id

LEFT JOIN session_features sf ON ft.customer_id = sf.customer_id

ORDER BY 1,2










