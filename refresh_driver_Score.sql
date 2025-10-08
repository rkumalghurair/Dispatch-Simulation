-- create  table test_ric.driver_metrics_scaling_oct_1  as

with week_ref AS (
SELECT 
  (date_trunc('week', current_date) - interval '1 day') + interval '1 day' AS latest_sunday_end,
  ((date_trunc('week', current_date) - interval '1 day') - interval '30 day') AS start_30day_period
)

,all_taxi_drivers as
( 
SELECT 
meterid,
usertype,
_id as ref_driver_id,
useruid as driver_id,
case when refusermaster ='67178c298f4c4d9d4657633e' then 'Kabi'
when refusermaster ='676125c8c9abe86ca466af9a'  then 'National Taxi'
when refusermaster='68b6f317881686da85284ea7' then 'DTC'
end as suppliername,
name
FROM public.users 
WHERE meterid is not null 
and usertype IN (2)
)

, jrny_base as 
(
select 
(journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as local_created_date
,EXTRACT(HOUR FROM (journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')as local_booking_hr
,TO_CHAR((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai', 'Day')as weekday
,journey_id
,ref_journey_id
,ref_parent_journey_id
,case when ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
then 'Taxi' else 'Limo' end as vehicle_cat
,ref_customer_id
,ref_driver_id
,journey_status
,journey_status_desc
,journey_created_at
,on_route_timestamp
,accepted_ride_timestamp
,arrived_at_pickup_timestamp
,journey_start_timestamp
,journey_completed_timestamp
,journey_cancel_timestamp
,ref_promo_applied
,actual_distance
,payment_mode
,dispatch_eta
,driver_pickup_latitude
,driver_pickup_longitude
,pickup_zone_name
,dispatch_latitude
,dispatch_longitude

,case when journey_status in (14) then  journey_id end as driver_cancelled
,case when journey_status in (13) then  journey_id end as customer_cancelled
,case when journey_status in (9,10) then journey_id end as completed_jrny
from 
prod_etl_data.tbl_journey_master  
where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-01-01'
and date ( (journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <  (SELECT latest_sunday_end FROM week_ref)
)


,base_2 as
(
select 
*
,date(date_trunc('week',local_created_date)) as booking_week
,case 
when vehicle_cat='Limo'  then accepted_ride_timestamp  
when vehicle_cat='Taxi' then on_route_timestamp end as journey_accepted_time
,case when
weekday in ('Monday','Tuesday','Wednesday','Thursday','Friday') and local_booking_hr in (8,9)then 'morning_super_peak'
when local_booking_hr in (16,17,18,19,20) then 'Evening_peak'
when weekday not in ('Monday','Tuesday','Wednesday','Thursday','Friday') and local_booking_hr in (8,9) then 'Morning_peak'
else 'Others' end as peak
from 
jrny_base
)
,base_3 as
(
select 
*,  case when ata <= (dispatch_eta + 2 ) then 1 else 0 end as ontime_jrny
from
    (
        select 
        *
        ,DATEDIFF(minute, journey_accepted_time, arrived_at_pickup_timestamp)as ata
        ,DATEDIFF(seconds, journey_accepted_time, journey_cancel_timestamp)as cancel_time_sec
        ,DATEDIFF (minute, arrived_at_pickup_timestamp, journey_completed_timestamp)as jrny_time
        from base_2
    )
)
,
jrny_ratings AS (
  SELECT ref_journey_id, ref_driver_id, rating, comment
  FROM prod_etl_data.tbl_userrating_details
)

,rating_details AS (
  SELECT ref_journey_id, ref_driver_id, LISTAGG(badge_name, ', ') AS badge_names
  FROM prod_etl_data.tbl_userrating_badges 
  GROUP BY 1,2
)
,rating_base AS 
(
  SELECT a.*, b.badge_names
  FROM jrny_ratings a
  LEFT JOIN rating_details b USING (ref_journey_id)
)
, final_base as
(
select 
base_3.* 
,r.rating
,r.badge_names
from base_3
left join rating_base  as r
on base_3.ref_journey_id =r.ref_journey_id
)
,driver_summary_lifetime as
(
select 
ref_driver_id
,count(distinct journey_id)as total_journeys
,count(distinct case when journey_status in (9,10) then journey_id end )as completed_journeys
,count(distinct case when journey_status in (14) then journey_id end )as driver_cancelled
,AVG(CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL 
         THEN rating::float8 END) AS avg_rating
,count(distinct case when ontime_jrny =1 and journey_status IN (9,10)  then journey_id end )as ontime_journeys
,count(distinct case when cancel_time_sec <30  and journey_status in (14) then journey_id end )as driver_cancelled_30s
from final_base 
group by 1
)

, driver_summary_30d AS (
  SELECT
    ref_driver_id,
    COUNT(DISTINCT journey_id) AS total_journeys_30d,
    COUNT(DISTINCT CASE WHEN journey_status IN (9,10) THEN journey_id END) AS completed_journeys_30d,
    COUNT(DISTINCT CASE WHEN journey_status = 14 THEN journey_id END) AS driver_cancelled_30d,
    COUNT(DISTINCT CASE WHEN ontime_jrny=1 and journey_status IN (9,10) THEN journey_id END) AS ontime_journeys_30d,
    COUNT(DISTINCT CASE WHEN cancel_time_sec <30 AND journey_status=14 THEN journey_id END) AS driver_cancelled_30s_30d,

    AVG(CASE WHEN journey_status IN (9,10) AND rating IS NOT NULL 
         THEN rating::float8 END) AS avg_rating_30d

  FROM final_base
  WHERE  
  local_created_date >= (SELECT start_30day_period FROM week_ref)
  AND local_created_date < (SELECT latest_sunday_end FROM week_ref)
GROUP BY ref_driver_id
)

,combined_data as 
(
SELECT 
  d.ref_driver_id,
    /* lifetime counts */
    COALESCE(lt.total_journeys,0)            AS total_journeys,
    COALESCE(lt.completed_journeys,0)        AS completed_journeys,
    COALESCE(lt.driver_cancelled,0)          AS driver_cancelled,
    COALESCE(lt.ontime_journeys,0)           AS ontime_journeys,
    COALESCE(lt.driver_cancelled_30s,0)      AS driver_cancelled_30s,
    lt.avg_rating,
    /* 30d counts */
    COALESCE(rc.total_journeys_30d,0)        AS total_journeys_30d,
    COALESCE(rc.completed_journeys_30d,0)    AS completed_journeys_30d,
    COALESCE(rc.driver_cancelled_30d,0)      AS driver_cancelled_30d,
    COALESCE(rc.ontime_journeys_30d,0)       AS ontime_journeys_30d,
    COALESCE(rc.driver_cancelled_30s_30d,0)  AS driver_cancelled_30s_30d,
    rc.avg_rating_30d

FROM all_taxi_drivers d
LEFT JOIN driver_summary_lifetime lt using(ref_driver_id)
LEFT JOIN driver_summary_30d rc on d.ref_driver_id =rc.ref_driver_id
)

-- ===== FINAL OUTPUT WITH % METRICS =====
,summary as
(
SELECT
  ref_driver_id,
  /* lifetime */
  total_journeys,
  completed_journeys,
  ROUND(100.0 * completed_journeys / NULLIF(total_journeys,0), 5)          AS completion_rate_pct,
  driver_cancelled,
  ROUND(100.0 * driver_cancelled / NULLIF(total_journeys,0), 5)            AS driver_cancel_rate_pct,
  coalesce(ROUND(avg_rating::numeric, 2)  ,5)                                          AS avg_rating,
  /* last 30 days */
  total_journeys_30d,
  completed_journeys_30d,
  ROUND(100.0 * completed_journeys_30d / NULLIF(total_journeys_30d,0), 5)  AS completion_rate_30d_pct,
  driver_cancelled_30d,
  ROUND(100.0 * driver_cancelled_30d / NULLIF(total_journeys_30d,0), 5)    AS driver_cancel_rate_30d_pct,
  coalesce(ROUND(avg_rating_30d::numeric, 2) ,5)                                       AS avg_rating_30d
FROM combined_data
)

, scoring AS (
  SELECT
    s.ref_driver_id,
    s.completion_rate_pct     AS cr_lt,
    coalesce(s.driver_cancel_rate_pct ,0)   AS dc_lt,
    COALESCE(s.avg_rating, 5)                 AS ar_lt,

    s.completion_rate_30d_pct    AS cr_rc,
    coalesce(s.driver_cancel_rate_30d_pct,0) AS dc_rc,
    COALESCE(s.avg_rating_30d, 5)             AS ar_rc,
    s.total_journeys,
    s.total_journeys_30d,

    CASE WHEN s.total_journeys < 5 THEN 1 ELSE 0 END AS lt_small,       -- force final=1 later
    CASE WHEN s.total_journeys_30d = 0 THEN 1 ELSE 0 END AS rc_missing  
  FROM summary s
)

, params AS 
(
  SELECT
    0.60::float AS m_cr,    0.30::float AS k_cr,     -- completion rate
     0.10::float AS m_dc,   12.0::float AS k_dc,   -- CANCEL rate (median~10%, steep)
     3.5::float  AS m_rt,   2.00::float AS k_rt  -- rating
)

, logistic_raw AS (
  SELECT f.*,

    -- lifetime
    1.0/(1.0+EXP(-p.k_cr*(GREATEST(LEAST(f.cr_lt/100.0,0.999999),0.000001)-p.m_cr)))           AS cr_log_lt_raw,
    1.0/(1.0+EXP( p.k_dc*(GREATEST(LEAST(f.dc_lt/100.0,0.999999),0.000001)-p.m_dc)))           AS dc_log_lt_raw,  -- note +k_dc


    -- recency
    1.0/(1.0+EXP(-p.k_cr*(GREATEST(LEAST(f.cr_rc/100.0,0.999999),0.000001)-p.m_cr)))           AS cr_log_rc_raw,
    1.0/(1.0+EXP( p.k_dc*(GREATEST(LEAST(f.dc_rc/100.0,0.999999),0.000001)-p.m_dc)))           AS dc_log_rc_raw,  -- note +k_dc

    -- rating logistic (lifetime)
    1.0 / (1.0 + EXP(-2.0 * (f.ar_lt - 3.5)))  AS rating_log_lt_raw,

-- rating logistic (recency)
    1.0 / (1.0 + EXP(-2.0 * (f.ar_rc - 3.5)))  AS rating_log_rc_raw

  FROM scoring f
  CROSS JOIN params p
)



, logistic_for_window AS (
  SELECT r.*,
         CASE WHEN lt_small=0 THEN cr_log_lt_raw     END AS cr_log_lt_win,
         CASE WHEN lt_small=0 THEN dc_log_lt_raw     END AS dc_log_lt_win,
         CASE WHEN lt_small=0 THEN rating_log_lt_raw END AS rating_log_lt_win,

         CASE WHEN rc_missing=0 THEN cr_log_rc_raw     END AS cr_log_rc_win,
         CASE WHEN rc_missing=0 THEN dc_log_rc_raw     END AS dc_log_rc_win,
         CASE WHEN rc_missing=0 THEN rating_log_rc_raw END AS rating_log_rc_win
  FROM logistic_raw r
)

, mm AS (
  SELECT
    MIN(cr_log_lt_win)     AS min_cr_lt,   MAX(cr_log_lt_win)     AS max_cr_lt,
    MIN(dc_log_lt_win)     AS min_dc_lt,   MAX(dc_log_lt_win)     AS max_dc_lt,
    MIN(rating_log_lt_win) AS min_rt_lt,   MAX(rating_log_lt_win) AS max_rt_lt,
    MIN(cr_log_rc_win)     AS min_cr_rc,   MAX(cr_log_rc_win)     AS max_cr_rc,
    MIN(dc_log_rc_win)     AS min_dc_rc,   MAX(dc_log_rc_win)     AS max_dc_rc,
    MIN(rating_log_rc_win) AS min_rt_rc,   MAX(rating_log_rc_win) AS max_rt_rc
  FROM logistic_for_window
)

, scaled AS (
  SELECT
    w.*,

    -- lifetime scaled to 0..1 using window min/max
    CASE 
      WHEN lt_small=1 THEN 1   -- force business rule
      ELSE (w.cr_log_lt_raw   - m.min_cr_lt  ) / NULLIF(m.max_cr_lt   - m.min_cr_lt  ,0)
    END AS cr_lt_scaled,


     CASE WHEN lt_small=1 THEN 1
         ELSE (w.dc_log_lt_raw - m.min_dc_lt)/NULLIF(m.max_dc_lt - m.min_dc_lt,0) END AS dc_lt_scaled,


    CASE 
      WHEN lt_small=1 THEN 1
      ELSE (w.rating_log_lt_raw - m.min_rt_lt) / NULLIF(m.max_rt_lt - m.min_rt_lt,0)
    END AS rt_lt_scaled,

    -- recency: if rc_missing=1 keep NULL so it can fall back to lifetime later
    CASE 
      WHEN rc_missing=1 or lt_small =1 THEN NULL
      ELSE (w.cr_log_rc_raw   - m.min_cr_rc  ) / NULLIF(m.max_cr_rc   - m.min_cr_rc  ,0)
    END AS cr_rc_scaled,

   CASE WHEN rc_missing=1 OR lt_small=1 THEN NULL
         ELSE (w.dc_log_rc_raw - m.min_dc_rc)/NULLIF(m.max_dc_rc - m.min_dc_rc,0) END AS dc_rc_scaled,


    CASE 
      WHEN rc_missing=1  or lt_small =1 THEN NULL
      ELSE (w.rating_log_rc_raw - m.min_rt_rc) / NULLIF(m.max_rt_rc - m.min_rt_rc,0)
    END AS rt_rc_scaled

  FROM logistic_for_window w
  CROSS JOIN mm m
)
 
, scored AS (
  SELECT
    sc.*,

    /* ---- Weighted sub-scores (0..1) ---- */
    (0.40 * cr_lt_scaled) +
    (0.40*dc_lt_scaled)+
    (0.20 * (rt_lt_scaled))   AS lifetime_score,

    (0.40 * cr_rc_scaled) +
    (0.40 * dc_rc_scaled) +
    (0.20 * rt_rc_scaled)   AS recency_score_raw
  FROM scaled sc
)
SELECT
  ref_driver_id,
    total_journeys,
    total_journeys_30d,
  /* raw metrics (optional to keep) */
  cr_lt   AS completion_rate_pct_lt,
  dc_lt   AS driver_cancel_rate_pct_lt,
  ar_lt   AS avg_rating_lt,
  cr_rc   AS completion_rate_pct_30d,
  dc_rc   AS driver_cancel_rate_pct_30d,
  ar_rc   AS avg_rating_30d,

  /* logistic components (0..1) */
  cr_lt_scaled,  dc_lt_scaled,  rt_lt_scaled,
  cr_rc_scaled,  dc_rc_scaled,  rt_rc_scaled,

  /* sub-scores (0..1) */
  ROUND(lifetime_score, 4)                                     AS lifetime_score,
  ROUND(CASE
          WHEN total_journeys_30d IS NULL OR total_journeys_30d = 0
               THEN lifetime_score
          ELSE recency_score_raw
        END, 4)                                                AS recency_score,

  ROUND( 0.40 * lifetime_score
       + 0.60 * COALESCE(
             CASE WHEN total_journeys_30d IS NULL OR total_journeys_30d = 0
                  THEN NULL
                  ELSE recency_score_raw
             END,
             lifetime_score),
        4)                                                     AS final_score,
        getdate() AT TIME ZONE 'Asia/Dubai' AS query_run_timestamp

FROM scored;
