 DROP TABLE  prod_etl_temp.po_metric_all;
CREATE  TABLE prod_etl_temp.po_metric_all AS

WITH master_base AS (
  SELECT
    DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')                    AS local_created_date,
    EXTRACT(HOUR FROM (journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')       AS local_booking_hr,
    TRIM(TO_CHAR((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai','Day'))     AS weekday,
    journey_id,
	ref_journey_id,
    ref_parent_journey_id,
    ref_customer_id,
    ref_driver_id,
    CASE WHEN ref_vehicle_category_id IN ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
         THEN 'Taxi' ELSE 'Limo' END                                                                      AS vehicle_cat,
    journey_status,
    journey_status_desc,
    journey_created_at,
    on_route_timestamp,
    accepted_ride_timestamp,
    arrived_at_pickup_timestamp,
    journey_start_timestamp,
    journey_completed_timestamp,
    COALESCE(dispatch_eta_sec, first_eta) AS dispatch_eta_sec,
    journey_cancel_timestamp,
    ref_promo_applied,
    actual_distance,
    actual_total_fee,
    estimate_total_fee,
    actual_base_fee,
    actual_total_distance_fee,
    actual_total_time_fee,
    driver_pickup_latitude,
    driver_pickup_longitude,
    dispatch_latitude,
    dispatch_longitude,
	transport_authority_job_id
    
  FROM 
  prod_etl_data.tbl_journey_master
  WHERE
   DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= current_date - INTERVAL '60 day' 
  AND DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < current_date
  -- DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')
  --       BETWEEN {{start_date}} AND {{end_date}}
)
,trap as 
(
SELECT 
distinct 
a.eventdatetime  as local_event_dt,
a.driverid as meterid,
a.jobnumber,
a.fareamount,
a.driverid||'-'||cast(jobnumber as varchar)||'-'||cast(eventdatetime as varchar) as uid,
case when (jobnumber=0 or jobnumber is null or jobnumber ='0') then 'Street'
when jobnumber in (select transport_authority_job_id from master_base where journey_status in (9,10,11,12,18)) then 'Zed'
else 'Others' end as trip_type
FROM rtaeventkafka as a 
WHERE 
date(eventdatetime) >=  current_date - INTERVAL '60 day' 
and date(eventdatetime) < current_date
-- date(eventdatetime) BETWEEN {{start_date}} AND {{end_date}}
and eventname='METER_VACANT'
)
,weekly_zed_share AS 
(
  SELECT
    DATE_TRUNC('week', local_event_dt)::date AS booking_week,
    (COUNT(DISTINCT CASE WHEN trip_type = 'Zed' THEN uid END)::double precision * 100.0
     / NULLIF(COUNT(DISTINCT uid),0)) AS pc_trips_zed
  FROM trap
  GROUP BY 1
)

,jrny_ratings as
(
select 
ref_journey_id
,ref_driver_id
,rating
,comment 
from prod_etl_data.tbl_userrating_details  
)
,rating_details
as 
(
select 
ref_journey_id
, ref_driver_id
,  LISTAGG(badge_name, ', ') AS badge_names 
from  
prod_etl_data.tbl_userrating_badges 
group by 1,2
)
,rating_base as
(
select
a.*
 
,b.badge_names
from 
jrny_ratings as a
left join 
rating_details as b
on a.ref_journey_id=b.ref_journey_id
)

,base_2 AS (
  SELECT
    master_base.*,
    CASE WHEN dispatch_eta_sec <= 210 THEN journey_id END AS trip_with_less_than_210_sec_eta,
    DATE_TRUNC('week', local_created_date)::date           AS booking_week,
    (DATE_TRUNC('week', local_created_date)::date + 6)     AS week_end,
    CASE WHEN vehicle_cat='Limo' THEN accepted_ride_timestamp
         WHEN vehicle_cat='Taxi' THEN on_route_timestamp END AS journey_accepted_time,
   
     CASE
      WHEN journey_status IN (9,10) AND vehicle_cat='Taxi' THEN
        CASE
          WHEN weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday') AND local_booking_hr IN (8,9) THEN 7
          WHEN weekday IN ('Monday','Tuesday','Wednesday','Thursday') AND local_booking_hr IN (16,17,18,19) THEN 7
          WHEN weekday IN ('Friday','Saturday','Sunday') AND local_booking_hr IN (16,17,18,19,20,21)   THEN 7
		  WHEN weekday IN ('Friday','Saturday','Sunday') AND local_booking_hr IN (22,23)   THEN 6.5
          WHEN weekday IN ('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday') AND local_booking_hr IN (0,1,2,3,4,5)               THEN 3.5
          WHEN weekday NOT IN ('Friday','Saturday','Sunday') AND local_booking_hr IN (22,23)     THEN 3.5
          ELSE 3
        END
      WHEN journey_status IN (9,10) AND vehicle_cat='Limo' THEN
        0.2 * (actual_base_fee + actual_total_distance_fee + actual_total_time_fee)
      ELSE NULL
    END AS revenue,
	 (
      6371 * ACOS(
        LEAST(
          1.0,
          COS(RADIANS(driver_pickup_latitude))
          * COS(RADIANS(dispatch_latitude))
          * COS(RADIANS(driver_pickup_longitude) - RADIANS(dispatch_longitude))
          + SIN(RADIANS(driver_pickup_latitude))
          * SIN(RADIANS(dispatch_latitude))
        )
      )
    ) AS haversine_distance_km_driver_pickup
	,rating
	,badge_names
  FROM master_base
  left join 
  rating_base 
  using (ref_journey_id )
   
)


,base_3 AS (
  SELECT
    *,
    DATEDIFF(second, journey_accepted_time, arrived_at_pickup_timestamp)        AS ata,
    DATEDIFF(second, journey_accepted_time, journey_cancel_timestamp)           AS cancel_time_sec,
    DATEDIFF(minute, arrived_at_pickup_timestamp, journey_completed_timestamp)  AS jrny_time
  FROM base_2
),

/* ========= FUNNEL (WEEKLY) ========= */
weekly AS (
  SELECT
    booking_week,
    COUNT( journey_id)                                                       AS total_journey_request,
    COUNT( CASE WHEN journey_status IN (9,10) THEN journey_id END)          AS completed_jrny,
	(COUNT(CASE WHEN journey_status IN (9,10) THEN journey_id END)::double precision / COUNT(journey_id)) * 100.0 AS completion_rate,
	
    COUNT( CASE WHEN journey_status NOT IN (9,10) THEN journey_id END)      AS cancelled_jrny,
	(COUNT( CASE WHEN journey_status NOT  IN (9,10) THEN journey_id END)::double precision /COUNT( journey_id))*100.0 as Cancellation_rate,
	
    COUNT( CASE WHEN journey_status = 14 THEN journey_id END)               AS driver_cancelled,
	
	   
    COUNT( CASE WHEN journey_status = 13 THEN journey_id END)               AS customer_cancelled,
	(COUNT( CASE WHEN journey_status = 14 THEN journey_id END) ::double precision/ COUNT( journey_id) )*100.0              AS driver_cancelled_rate,
	(COUNT( CASE WHEN journey_status = 13 THEN journey_id END) ::double precision/ COUNT( journey_id))*100.0 as customer_Cancelled_rate,
	
    COUNT( CASE WHEN journey_status IN (15,19) THEN journey_id END)         AS admin_cancelled,
    COUNT( CASE WHEN ref_driver_id IS NULL THEN journey_id END)             AS driver_not_found,
    COUNT( ref_customer_id)                                                 AS active_customer_cnt,
    AVG(Round (dispatch_eta_sec/60   :: numeric,2) )                                                       AS avg_dispatch_eta,
    AVG(Round (ata/60   :: numeric,2) )                                                                     AS avg_ata,
    AVG(haversine_distance_km_driver_pickup)                                        AS avg_haversine_distance_km_driver_pickup,
    COUNT( CASE WHEN dispatch_eta_sec <= 210 THEN journey_id END)           AS trips_with_less_than_3_5mins_eta,
	 
    COUNT( CASE WHEN ref_promo_applied IS NOT NULL THEN journey_id END)     AS promo_jrny_cnt,
	SUM(COALESCE(revenue,0))                                                        AS zed_revenue,


	count( case when ref_driver_id is  null then journey_id end ) ::double precision/ COUNT( journey_id)       AS driver_not_found_pct,

    COUNT( CASE WHEN journey_status IN (15,19)  THEN journey_id END) ::double precision/ COUNT( journey_id)     AS admin_cancelled_pct,

    count( case when ref_promo_applied is not null then journey_id end)::double precision / COUNT( journey_id)    AS promo_jrny_pct,

    (count( trip_with_less_than_210_sec_eta)::double precision/ COUNT(DISTINCT journey_id)  )  *100.0            AS trips_lt_210_sec_eta_pct,

	 ( COUNT( CASE WHEN ata <= 210 THEN journey_id END) ::double precision/ COUNT(DISTINCT case when journey_status IN (9,10) THEN journey_id end)  )  *100.0            AS trips_lt_210_sec_ata_pct,
    
    COUNT( CASE WHEN  ABS(ata - dispatch_eta_sec) <= 120 THEN journey_id END)::double precision/ COUNT(  journey_id)   AS pickup_eta_accuracy_within_2mins_pct,

	 COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND rating >= 4 THEN journey_id END ) as numerator_jrny_ratings,
	 COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND (
        badge_names IS NULL
        OR (
             badge_names NOT ILIKE '%Dirty car%'
         AND badge_names NOT ILIKE '%Bad odour%'
         AND badge_names NOT ILIKE '%Unfriendly chat%'
        )
		) then journey_id end ) as denominator_jrny_ratings,

		 COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND rating >= 4 THEN journey_id END ) ::double precision  /COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND (
        badge_names IS NULL
        OR (
             badge_names NOT ILIKE '%Dirty car%'
         AND badge_names NOT ILIKE '%Bad odour%'
         AND badge_names NOT ILIKE '%Unfriendly chat%'
        )
		) then journey_id end ) as pct_customer_ratings_greater_than_4
	 
	
  FROM base_3
  GROUP BY 1
),

/* ========= FUNNEL (MTD for month of {{end_date}}) ========= */
mtd AS (
  SELECT
    'MTD'::text                                                                      AS period_label,
    COUNT( journey_id)                                                       AS total_journey_request,
    COUNT( CASE WHEN journey_status IN (9,10) THEN journey_id END)          AS completed_jrny,
	(COUNT(CASE WHEN journey_status IN (9,10) THEN journey_id END)::double precision / COUNT(journey_id)) * 100.0 AS completion_rate,
    COUNT( CASE WHEN journey_status NOT IN (9,10) THEN journey_id END)      AS cancelled_jrny,
	(COUNT( CASE WHEN journey_status NOT  IN (9,10) THEN journey_id END)::double precision /COUNT( journey_id))*100.0 as Cancellation_rate,
    COUNT( CASE WHEN journey_status = 14 THEN journey_id END)               AS driver_cancelled,
    COUNT( CASE WHEN journey_status = 13 THEN journey_id END)               AS customer_cancelled,
	(COUNT( CASE WHEN journey_status = 14 THEN journey_id END) ::double precision/ COUNT( journey_id) )*100.0              AS driver_cancelled_pct,
	(COUNT( CASE WHEN journey_status = 13 THEN journey_id END) ::double precision/ COUNT( journey_id))*100.0 as customer_Cancelled_pct,
    COUNT( CASE WHEN journey_status IN (15,19) THEN journey_id END)         AS admin_cancelled,
    COUNT( CASE WHEN ref_driver_id IS NULL THEN journey_id END)             AS driver_not_found,
    COUNT( ref_customer_id)                                                 AS active_customer_cnt,
    AVG(Round (dispatch_eta_sec/60   :: numeric,2) )                                                       AS avg_dispatch_eta,
    AVG(Round (ata/60   :: numeric,2) )                                                                     AS avg_ata,
    AVG(haversine_distance_km_driver_pickup)                                        AS avg_haversine_distance_km_driver_pickup,
    COUNT( CASE WHEN dispatch_eta_sec <= 210 THEN journey_id END)           AS trips_with_less_than_3_5mins_eta,
    COUNT( CASE WHEN ref_promo_applied IS NOT NULL THEN journey_id END)     AS promo_jrny_cnt,
	 SUM(COALESCE(revenue,0))                                                        AS zed_revenue,


    count( case when ref_driver_id is  null then journey_id end ) ::double precision/ COUNT( journey_id)       AS driver_not_found_pct,

    COUNT( CASE WHEN journey_status IN (15,19)  THEN journey_id END) ::double precision/ COUNT( journey_id)     AS admin_cancelled_pct,

    count( case when ref_promo_applied is not null then journey_id end)::double precision / COUNT( journey_id)    AS promo_jrny_pct,

    (count( trip_with_less_than_210_sec_eta)::double precision/ COUNT(DISTINCT journey_id) )* 100.0            AS trips_lt_210_sec_eta_pct,

	 ( COUNT( CASE WHEN ata <= 210 THEN journey_id END) ::double precision/ COUNT( case when journey_status IN (9,10) THEN journey_id end)  )  *100.0            AS trips_lt_210_sec_ata_pct,
    
    COUNT( CASE WHEN  ABS(ata - dispatch_eta_sec) <= 120 THEN journey_id END)::double precision/ COUNT(  journey_id)   AS pickup_eta_accuracy_within_2mins_pct,

	 COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND rating >= 4 THEN journey_id END ) as numerator_jrny_ratings,
	 COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND (
        badge_names IS NULL
        OR (
             badge_names NOT ILIKE '%Dirty car%'
         AND badge_names NOT ILIKE '%Bad odour%'
         AND badge_names NOT ILIKE '%Unfriendly chat%'
        )
		) then journey_id end ) as denominator_jrny_ratings,

		 COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND rating >= 4 THEN journey_id END )::double precision /COUNT(DISTINCT CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL AND (
        badge_names IS NULL
        OR (
             badge_names NOT ILIKE '%Dirty car%'
         AND badge_names NOT ILIKE '%Bad odour%'
         AND badge_names NOT ILIKE '%Unfriendly chat%'
        )
		) then journey_id end ) as pct_customer_ratings_greater_than_4


   
  FROM base_3
  WHERE   local_created_date BETWEEN DATE_TRUNC('month',  current_date   ) AND current_Date
  -- local_created_date BETWEEN DATE_TRUNC('month', {{end_date}}::date) AND {{end_date}}::date
)

,mtd_zed_share AS (
  SELECT
    'MTD'::text AS period_label,
    (COUNT(DISTINCT CASE WHEN trip_type = 'Zed' THEN uid END)::double precision * 100.0
     / NULLIF(COUNT(DISTINCT uid),0)) AS pc_trips_zed
  FROM trap

  WHERE local_event_dt BETWEEN DATE_TRUNC('month', current_Date::date) AND current_Date::date
 

  -- WHERE local_event_dt BETWEEN DATE_TRUNC('month', {{end_date}}::date) AND {{end_date}}::date
)

/* ========= USER BUCKETS (first-ever completed date up to end_date) ========= */
,minimum_date AS (
  SELECT
    ref_customer_id,
    MIN(DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')) AS min_completed_jrny_date
  FROM prod_etl_data.tbl_journey_master
  WHERE journey_status IN (9,10)
   AND DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <= current_Date
    -- AND DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <= {{end_date}}
  GROUP BY 1
),

/* Per-week trip counts per user */
weekly_user_freq AS (
  SELECT
    b.booking_week,
    b.ref_customer_id,
    m.min_completed_jrny_date,
    COUNT( CASE WHEN b.journey_status IN (9,10) THEN b.journey_id END) AS trips_in_week
  FROM base_2 b
  LEFT JOIN minimum_date m USING (ref_customer_id)
  WHERE b.journey_status IN (9,10)
  GROUP BY 1,2,3
),

weekly_user_rollup AS (
  SELECT
    booking_week,
    COUNT(DISTINCT ref_customer_id) AS active_users_count,
    COUNT(DISTINCT CASE WHEN min_completed_jrny_date BETWEEN booking_week AND (booking_week + 6) THEN ref_customer_id END) AS new_users_count,
    COUNT(DISTINCT CASE WHEN min_completed_jrny_date < booking_week THEN ref_customer_id END)                               AS existing_users_count,
  

	ROUND(AVG((trips_in_week ::numeric(18,3))), 1) AS avg_trip_freq_active_users,
    ROUND(AVG(CASE WHEN min_completed_jrny_date BETWEEN booking_week AND (booking_week + 6) THEN (trips_in_week ::numeric(18,3)) END) ,1)             AS avg_trip_freq_new_users,
    ROUND( AVG(CASE WHEN min_completed_jrny_date < booking_week THEN (trips_in_week::numeric(18,3)) END)  ,1)                                         AS avg_trip_freq_existing_users
  FROM weekly_user_freq
  GROUP BY 1
),

/* MTD user buckets within the month of {{end_date}} */
mtd_user_freq AS (
  SELECT
    DATE_TRUNC('month', current_Date)::date AS month_start,
    b.ref_customer_id,
    m.min_completed_jrny_date,
    COUNT(DISTINCT CASE WHEN b.journey_status IN (9,10) THEN b.journey_id END) AS trips_in_mtd
  FROM base_2 b
  LEFT JOIN minimum_date m USING (ref_customer_id)
  WHERE b.journey_status IN (9,10)
    AND b.local_created_date BETWEEN DATE_TRUNC('month', current_Date::date) AND current_Date ::date
  GROUP BY 1,2,3
),

mtd_user_rollup AS (
  SELECT
    month_start,
    COUNT(DISTINCT ref_customer_id) AS active_users_count,
    COUNT(DISTINCT CASE WHEN DATE_TRUNC('month', min_completed_jrny_date) = month_start THEN ref_customer_id END) AS new_users_count,
    COUNT(DISTINCT CASE WHEN min_completed_jrny_date < month_start THEN ref_customer_id END)                       AS existing_users_count,
	ROUND(AVG((trips_in_mtd ::numeric(18,3))), 1) AS avg_trip_freq_active_users,
    ROUND(AVG(CASE WHEN DATE_TRUNC('month', min_completed_jrny_date) = month_start THEN (trips_in_mtd ::numeric(18,3)) END),1)               AS avg_trip_freq_new_users,
    ROUND(AVG(CASE WHEN min_completed_jrny_date < month_start THEN (trips_in_mtd ::numeric(18,3)) END)  ,1)                                  AS avg_trip_freq_existing_users
  FROM mtd_user_freq
  GROUP BY 1
),

/* ======== Amplitude sessions ======== */
-- amp_events AS (
--   SELECT
--     DATE((event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS local_event_dt,
--     session_id,
--     event_type
--   FROM amplitude_customer_app.events
--   WHERE session_id <> -1
--     AND DATE((event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')

--         BETWEEN current_date - INTERVAL '30 day' and CURRENT_DATE
--         -- BETWEEN {{start_date}} AND {{end_date}}
-- ),
-- total_sessions_daily AS (
--   SELECT local_event_dt, COUNT(DISTINCT session_id) AS total_session_cnt
--   FROM amp_events
--   GROUP BY 1
-- ),
-- booked_sessions_daily AS (
--   SELECT local_event_dt, COUNT(DISTINCT session_id) AS booked_session_cnt
--   FROM amp_events
--   WHERE event_type = 'book_journey_clicked'
--   GROUP BY 1
-- ),
-- session_conv_weekly AS (
--   SELECT
--     DATE_TRUNC('week', d.local_event_dt)::date AS week_start,
--     SUM(d.total_session_cnt)                   AS total_sessions,
--     SUM(COALESCE(b.booked_session_cnt,0))      AS booked_sessions,
--     (CASE WHEN SUM(d.total_session_cnt) > 0
--          THEN SUM(COALESCE(b.booked_session_cnt,0))::double precision / SUM(d.total_session_cnt)
--     END)*100.0 AS session_conv_rate
--   FROM total_sessions_daily d
--   LEFT JOIN booked_sessions_daily b USING (local_event_dt)
--   GROUP BY 1
-- ),
session_conv_weekly AS 
(
select 
DATE_TRUNC('week', d.local_event_dt)::date AS week_start
,sum(total_booked_session) AS booked_sessions
,sum(total_page_view_session_cnt) as total_sessions
,(CASE WHEN SUM(d.total_page_view_session_cnt) > 0 THEN SUM(COALESCE(d.total_booked_session,0))::double precision / SUM(d.total_page_view_session_cnt)END)*100.0 AS session_conv_rate
 from 
 prod_etl_temp.amplitude_daily_agg d 
 where local_event_dt >= current_date - INTERVAL '60 day' 
 and local_event_dt <current_date
 group by 1
),

session_conv_mtd AS (
  SELECT
    'MTD'::text AS period_label,
	SUM(d.total_page_view_session_cnt)   as total_sessions,
    (CASE WHEN SUM(d.total_page_view_session_cnt) > 0
         THEN SUM(COALESCE(d.total_booked_session,0))::double precision / SUM(d.total_page_view_session_cnt)
    END)*100.0 AS session_conv_rate
  FROM prod_etl_temp.amplitude_daily_agg d 
  WHERE d.local_event_dt BETWEEN   DATE_TRUNC('month', current_Date ::date) AND current_Date::date
  -- DATE_TRUNC('month', {{end_date}}::date) AND {{end_date}}::date
),

/* ========= LONG FORMAT (Weekly + MTD) ========= */
unioned AS (

  /* ── WEEKLY ── */
  SELECT
    TO_CHAR(w.booking_week,'YYYY-MM-DD') AS period,
    m.metric_name                        AS metric,
    m.metric_order                       AS metric_order,
    CASE m.metric_order
      WHEN 1  THEN w.total_journey_request::double precision
      WHEN 2  THEN w.completed_jrny::double precision
      WHEN 3  THEN w.completion_rate::double precision
      -- WHEN 4  THEN w.cancelled_jrny::double precision
      WHEN 5  THEN w.cancellation_rate::double precision
      -- WHEN 6  THEN w.driver_cancelled::double precision
      -- WHEN 7  THEN w.customer_cancelled::double precision
      WHEN 6  THEN w.zed_revenue::double precision
      -- WHEN 8  THEN w.admin_cancelled::double precision
      -- WHEN 9  THEN w.driver_not_found::double precision
      WHEN 10 THEN w.driver_cancelled_rate
      WHEN 11 THEN w.customer_cancelled_rate
      -- WHEN 12 THEN w.driver_not_found_pct
      -- WHEN 13 THEN w.admin_cancelled_pct
      -- WHEN 15 THEN w.promo_jrny_pct
      -- WHEN 16 THEN w.active_customer_cnt::double precision
      WHEN 17 THEN w.avg_dispatch_eta::double precision
      WHEN 18 THEN w.avg_ata::double precision
      WHEN 30 THEN w.pct_customer_ratings_greater_than_4 * 100.0
      -- WHEN 19 THEN w.avg_haversine_distance_km_driver_pickup::double precision
      WHEN 31 THEN w.trips_lt_210_sec_eta_pct ::double precision
      WHEN 32 THEN w.trips_lt_210_sec_ata_pct ::double precision
      -- WHEN 21 THEN w.pickup_eta_accuracy_within_2mins_pct
      WHEN 33 THEN wz.pc_trips_zed::double precision
    END AS value
  FROM weekly w
  LEFT JOIN weekly_zed_share wz
    ON wz.booking_week = w.booking_week
  CROSS JOIN (
    SELECT 1  AS metric_order, 'Total Journey Request'                           AS metric_name UNION ALL
    SELECT 2 , 'Total Completed Journey'                                         UNION ALL
    SELECT 3 , 'Total Completion Rate'                                           UNION ALL
    -- SELECT 4 , 'Total Cancelled Journey'                                         UNION ALL
    SELECT 5 , 'Total Cancellation Rate'                                         UNION ALL
    -- SELECT 6 , 'Driver Cancelled'                                               UNION ALL
    -- SELECT 7 , 'Customer Cancelled'                                              UNION ALL
    SELECT 6,  'Total ZED Revenue'                                               UNION ALL
    -- SELECT 8 , 'Admin Cancelled'                                                UNION ALL
    -- SELECT 9 , 'Unfullfilled Jrny Count'                                         UNION ALL
    SELECT 10, 'Driver Cancelled %'                                              UNION ALL
    SELECT 11, 'Customer Cancelled %'                                            UNION ALL
    -- SELECT 12, 'Unfulfilled%'                                                  UNION ALL
    -- SELECT 13, 'Admin Cancelled %'                                             UNION ALL
    -- SELECT 15, 'Promo Journey %'                                               UNION ALL
    -- SELECT 16, 'Active Users Count'                                            UNION ALL
    SELECT 17, 'Avg Dispatch ETA'                                                UNION ALL
    SELECT 18, 'Avg ATA'                                                         UNION ALL
    SELECT 30, 'Customer Ratings ≥ 4 %'                                          UNION ALL
    -- SELECT 19, 'Avg Distance Driver_Pickup_km'                                 UNION ALL
    SELECT 31, '≤ 3.5 Minutes ETA %'                                            UNION ALL
    SELECT 32, '≤ 3.5 Minutes ATA %'                                            UNION ALL
    SELECT 33, 'ZED % Share'
  ) m(metric_order, metric_name)

  UNION ALL

  /* ── WEEKLY USER ROLLUPS ── */
  SELECT
    TO_CHAR(u.booking_week,'YYYY-MM-DD') AS period,
    m.metric_name                        AS metric,
    m.metric_order                       AS metric_order,
    CASE m.metric_order
      -- WHEN 22 THEN u.active_users_count::double precision
      WHEN 23 THEN u.new_users_count::double precision
      WHEN 24 THEN u.existing_users_count::double precision
      WHEN 25 THEN u.avg_trip_freq_active_users::double precision
      -- WHEN 26 THEN u.avg_trip_freq_new_users::double precision
      -- WHEN 27 THEN u.avg_trip_freq_existing_users::double precision
    END AS value
  FROM weekly_user_rollup u
  CROSS JOIN (
    -- SELECT 22 AS metric_order, 'active_users_count'                     AS metric_name UNION ALL
    SELECT 23, 'New User Count'                                                     UNION ALL
    SELECT 24, 'Existing User Count'                                                UNION ALL
    SELECT 25, 'Avg Trip Frequency Active Users'
    -- UNION ALL
    -- SELECT 26, 'Avg Trip Frequency New Users'                                     UNION ALL
    -- SELECT 27, 'Avg Trip Frequency Existing Users'
  ) m(metric_order, metric_name)

  UNION ALL

  /* ── WEEKLY SESSIONS ── */
  SELECT
    TO_CHAR(s.week_start,'YYYY-MM-DD') AS period,
    m.metric_name                      AS metric,
    m.metric_order                     AS metric_order,
    CASE m.metric_order
      WHEN 28 THEN s.total_sessions::double precision
      WHEN 29 THEN s.session_conv_rate
    END AS value
  FROM session_conv_weekly s
  CROSS JOIN (
    SELECT 28 AS metric_order, 'Total Unique Session Count' AS metric_name UNION ALL
    SELECT 29, 'Session Conversion Rate'
  ) m(metric_order, metric_name)

  UNION ALL

  /* ── MTD ── */
  SELECT
    'MTD'                             AS period,
    m.metric_name                     AS metric,
    m.metric_order                    AS metric_order,
    CASE m.metric_order
      WHEN 1  THEN t.total_journey_request::double precision
      WHEN 2  THEN t.completed_jrny::double precision
      WHEN 3  THEN t.completion_rate::double precision
      -- WHEN 4  THEN t.cancelled_jrny::double precision
      WHEN 5  THEN t.cancellation_rate::double precision
      -- WHEN 6  THEN t.driver_cancelled::double precision
      -- WHEN 7  THEN t.customer_cancelled::double precision
      WHEN 6  THEN t.zed_revenue::double precision
      -- WHEN 8  THEN t.admin_cancelled::double precision
      -- WHEN 9  THEN t.driver_not_found::double precision
      WHEN 10 THEN t.driver_cancelled_pct
      WHEN 11 THEN t.customer_cancelled_pct
      -- WHEN 12 THEN t.driver_not_found_pct
      -- WHEN 13 THEN t.admin_cancelled_pct
      -- WHEN 15 THEN t.promo_jrny_pct
      -- WHEN 16 THEN t.active_customer_cnt::double precision
      WHEN 17 THEN t.avg_dispatch_eta::double precision
      WHEN 18 THEN t.avg_ata::double precision
      WHEN 30 THEN t.pct_customer_ratings_greater_than_4 * 100.0
      -- WHEN 19 THEN t.avg_haversine_distance_km_driver_pickup::double precision
      WHEN 31 THEN t.trips_lt_210_sec_eta_pct::double precision
      WHEN 32 THEN t.trips_lt_210_sec_ata_pct ::double precision
      -- WHEN 21 THEN t.pickup_eta_accuracy_within_2mins_pct
      WHEN 33 THEN mz.pc_trips_zed::double precision
    END AS value
  FROM mtd t
  CROSS JOIN mtd_zed_share mz
  CROSS JOIN (
    SELECT 1  AS metric_order, 'Total Journey Request'                           AS metric_name UNION ALL
    SELECT 2 , 'Total Completed Journey'                                         UNION ALL
    SELECT 3 , 'Total Completion Rate'                                           UNION ALL
    -- SELECT 4 , 'Total Cancelled Journey'                                       UNION ALL
    SELECT 5 , 'Total Cancellation Rate'                                         UNION ALL
    -- SELECT 6 , 'Driver Cancelled'                                              UNION ALL
    -- SELECT 7 , 'Customer Cancelled'                                            UNION ALL
    SELECT 6,  'Total ZED Revenue'                                               UNION ALL
    -- SELECT 8 , 'Admin Cancelled'                                               UNION ALL
    -- SELECT 9 , 'Unfullfilled Jrny Count'                                       UNION ALL
    SELECT 10, 'Driver Cancelled %'                                              UNION ALL
    SELECT 11, 'Customer Cancelled %'                                            UNION ALL
    -- SELECT 12, 'Unfulfilled%'                                                  UNION ALL
    -- SELECT 13, 'Admin Cancelled %'                                             UNION ALL
    -- SELECT 15, 'Promo Journey %'                                               UNION ALL
    -- SELECT 16, 'Active Users Count'                                            UNION ALL
    SELECT 17, 'Avg Dispatch ETA'                                                UNION ALL
    SELECT 18, 'Avg ATA'                                                         UNION ALL
    SELECT 30, 'Customer Ratings ≥ 4 %'                                          UNION ALL
    -- SELECT 19, 'Avg Distance Driver_Pickup_km'                                 UNION ALL
    SELECT 31, '≤ 3.5 Minutes ETA %'                                            UNION ALL
    SELECT 32, '≤ 3.5 Minutes ATA %'                                            UNION ALL
    SELECT 33, 'ZED % Share'
  ) m(metric_order, metric_name)

  UNION ALL

  /* ── MTD USER ROLLUPS ── */
  SELECT
    'MTD'                           AS period,
    m.metric_name                   AS metric,
    m.metric_order                  AS metric_order,
    CASE m.metric_order
      -- WHEN 22 THEN r.active_users_count::double precision
      WHEN 23 THEN r.new_users_count::double precision
      WHEN 24 THEN r.existing_users_count::double precision
      WHEN 25 THEN r.avg_trip_freq_active_users::double precision
      -- WHEN 26 THEN r.avg_trip_freq_new_users::double precision
      -- WHEN 27 THEN r.avg_trip_freq_existing_users::double precision
    END AS value
  FROM mtd_user_rollup r
  CROSS JOIN (
    -- SELECT 22 AS metric_order, 'Active Users Count'                  AS metric_name UNION ALL
    SELECT 23, 'New User Count'                                                   UNION ALL
    SELECT 24, 'Existing User Count'                                              UNION ALL
    SELECT 25, 'Avg Trip Frequency Active Users'
    -- UNION ALL
    -- SELECT 26, 'Avg Trip Frequency New Users'                                   UNION ALL
    -- SELECT 27, 'Avg Trip Frequency Existing Users'
  ) m(metric_order, metric_name)

  UNION ALL
  /* ── MTD SESSIONS ── */
  SELECT
    'MTD'                         AS period,
    m.metric_name                 AS metric,
    m.metric_order                AS metric_order,
    CASE m.metric_order
      WHEN 28 THEN s.total_sessions::double precision
      WHEN 29 THEN s.session_conv_rate
    END AS value
  FROM session_conv_mtd s
  CROSS JOIN (
    SELECT 28 AS metric_order, 'Total Unique Session Count' AS metric_name UNION ALL
    SELECT 29, 'Session Conversion Rate'
  ) m(metric_order, metric_name)


)

,monthly_targets AS 
(
  SELECT 1 as metric_order, 'Total Journey Request' AS metric, 266815::double precision AS value UNION ALL
  SELECT 2 as metric_order,'Total Completed Journey', 160089 UNION ALL
  SELECT 3 as  metric_order, 'Total Completion Rate', 60.0 UNION ALL 
  SELECT 5 as metric_order, 'Total Cancellation Rate', 40.0 UNION ALL
  
  SELECT 6 as metric_order, 'Total ZED Revenue', 0 UNION ALL

  SELECT 10 as metric_order, 'Driver Cancelled %',10.0  UNION ALL
  SELECT 11 as metric_order, 'Customer Cancelled %', 30.0 UNION ALL

  SELECT 17 as metric_order, 'Avg Dispatch ETA', 3.5 UNION ALL
  SELECT  18 as metric_order, 'Avg ATA', 3.5 UNION ALL

  SELECT 30 as metric_order, 'Customer Ratings ≥ 4 %', 98.0 UNION ALL
  
  SELECT 31 as metric_order, '≤ 3.5 Minutes ETA %', 0 UNION ALL
  SELECT 32 as metric_order, '≤ 3.5 Minutes ATA %', 0 UNION ALL
  SELECT 33 as metric_order, 'ZED % Share', 0 UNION ALL
    
  SELECT 23 as metric_order, 'New User Count', 28601  UNION ALL
  SELECT 24 as metric_order, 'Existing User Count', 23696 UNION ALL
  SELECT 25 as metric_order, 'Avg Trip Frequency Active Users' , 3.16 UNION ALL 
 
  SELECT 28 as metric_order, 'Total Unique Session Count',  607341 UNION ALL
  SELECT 29 as metric_order, 'Session Conversion Rate' , 37.0 
 
  
)

,final_ranked AS (
  SELECT
    period::text                   AS period,
    metric::text                   AS metric,
    SUM(value::double precision)   AS value,
    MIN(metric_order)              AS metric_order
  FROM unioned
  GROUP BY 1,2

)


-- SELECT
-- *
-- FROM final_ranked
-- -- GROUP BY 1,2
-- ORDER BY period,metric_order

SELECT
  period,
  metric,
  value,
  metric_order
FROM final_ranked

UNION ALL

SELECT
  'Monthly Target' AS period,
  metric,
  value,
  metric_order
FROM monthly_targets

ORDER BY  period ,metric_order;

