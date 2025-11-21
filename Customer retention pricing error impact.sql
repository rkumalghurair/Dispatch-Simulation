WITH cust_details as(
SELECT 
usertype,
_id as ref_customer_id,
useruid as customer_id,
name,
countrycode,
mobilenumber,
createdat,
emailid
FROM public.users 
where usertype IN (1)
)
-- details of customer raising support ticket
, customer_support as
(
select 
date, 
initiated_at
, resolved_at
, chat_type
, csat_score
, user_properties_phone_number
, user_properties_name 
, user_properties_email
from "upload_chats_data_4_months_20251110062008" 
)
,joined_details as
(
select 
tkts.*, cust.* 
,TO_TIMESTAMP(initiated_at, 'MM/DD/YYYY HH24:MI') AS initiated_date
from
customer_support as tkts
left join 
cust_details as cust
on tkts.user_properties_phone_number = cust.mobilenumber
where mobilenumber is not null 
)
-- select count(*), count(distinct  ref_customer_id) from joined_details;

,base AS (
  SELECT
      j.journey_id,
      j.ref_journey_id,
      j.ref_customer_id,
      j.journey_status,
      (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
      DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS journey_dt,
      j.pickup_latitude,
      j.pickup_longitude,
      j.actual_drop_off_latitude,
      j.actual_drop_off_longitude,
      j.estimate_drop_off_latitude,
      j.estimate_drop_off_longitude,
      j.estimate_total_fee,
      j.actual_total_fee
  FROM prod_etl_data.tbl_journey_master j
  WHERE j.journey_status IN (9,10)  
  and ref_vehiclecatgeory_id =''                    -- completed trips only 
    AND DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') BETWEEN '2025-09-01' AND '2025-11-20'
)
-- select count(*),  count(distinct journey_id )from base; --  299,237

,loc_clean AS (
  SELECT
    b.*,

    ST_DistanceSphere(
        ST_MakePoint(estimate_drop_off_longitude, estimate_drop_off_latitude),
        ST_MakePoint(actual_drop_off_longitude, actual_drop_off_latitude)
    ) AS drop_dist_m,

    (b.actual_total_fee - b.estimate_total_fee)::NUMERIC
      / NULLIF(b.estimate_total_fee,0) AS price_error
  FROM base b
)
,mispriced AS (
  SELECT
    *
  FROM loc_clean
--   WHERE drop_dist_m <= 50   --- filter for drop distance parity
--   AND estimate_total_fee > 0

)

-- 131,575
,bucketed AS (
  SELECT
    m.*,
    CASE
      WHEN price_error <= 0  THEN '0.less_than 0'
      WHEN price_error <= 0.05 THEN '1.0–5%'
      WHEN price_error <= 0.10 THEN '2.5–10%'
      WHEN price_error <= 0.15 THEN '3.10–15%'
      WHEN price_error <= 0.20 THEN '4.15–20%'
      ELSE '5.>20%'
    END AS price_bucket
    , case when actual_total_fee <=30 then '0.Less_than_30'
    when actual_total_fee <=50 then '1.30_50'
    when actual_total_fee <=70 then '2.50_70'
    when actual_total_fee >70 then '3>70'
    end as actual_fee_bkt
  FROM mispriced m
) 
,full_history AS (
    SELECT
        ref_customer_id,
        journey_id,
        ref_journey_id,
        (journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
        ROW_NUMBER() OVER (
            PARTITION BY ref_customer_id  ORDER BY (journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') - 1 AS prior_completed_trips
    FROM prod_etl_data.tbl_journey_master
    WHERE journey_status IN (9,10)
     AND DATE((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')<= '2025-11-20'
)
-- select * from  full_history where ref_customer_id in ('65dc79fab17ddd0b3480a304','65dc79fab17ddd0b3480a376') order by ref_customer_id;

,customer_lifetime AS (
    SELECT
        ref_customer_id,
        COUNT(journey_id) AS lifetime_completed_trips
    FROM full_history
    WHERE journey_ts < '2025-09-01'    -- before analysis window
    GROUP BY 1
)

,window_trips AS (
    SELECT
        *
    FROM full_history 
    WHERE 
       1=1
        And journey_ts BETWEEN '2025-09-01' AND '2025-11-20'
),
ranked_window AS (
    SELECT
        w.*,
        c.lifetime_completed_trips,
        ROW_NUMBER() OVER (PARTITION BY w.ref_customer_id ORDER BY w.journey_ts) AS window_trip_index
    FROM window_trips w
    LEFT JOIN customer_lifetime c USING (ref_customer_id)
)
-- select * from ranked_window;
,
final_history AS (
    SELECT
        *
        -- (lifetime_completed_trips + window_trip_index - 1) AS prior_completed_trips
    FROM ranked_window
)
-- SELECT * FROM final_history;
,cohorted AS (
  SELECT
    fh.*,
    CASE
      WHEN prior_completed_trips = 0 THEN '0.new_0'
      WHEN prior_completed_trips BETWEEN 1 AND 4 THEN '1.2–5'
      WHEN prior_completed_trips BETWEEN 5 AND 9 THEN '2.6–10'
      ELSE '3.10Plus'
    END AS experience_cohort
  FROM final_history fh
)
-- select * from cohorted;
,ratings AS (
  SELECT
    ref_journey_id,
    rating
  FROM prod_etl_data.tbl_userrating_details
),

complaints AS (
  -- assuming your joined_details CTE already exists as in your code
  SELECT
    jd.customer_id,
    jd.ref_customer_id,
    jd.initiated_date::timestamp AS complaint_ts
  FROM joined_details jd
   qualify row_number () over (partition by customer_id, initiated_date )=1
)
-- select * from complaints;
,enriched AS (
  SELECT
    c.*,
    r.rating,
    CASE WHEN r.rating IS NOT NULL AND r.rating < 3 THEN 1 ELSE 0 END AS low_rating_lt3,
    CASE WHEN r.rating IS NOT NULL AND r.rating < 4 THEN 1 ELSE 0 END AS low_rating_lt4,

    -- complaint within 24h of the trip
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM complaints comp
        WHERE comp.ref_customer_id = c.ref_customer_id     -- adjust key if you map phone/email
          AND comp.complaint_ts BETWEEN c.journey_ts
                                     AND c.journey_ts + INTERVAL '24 hour') THEN 1 ELSE 0
    END AS has_complaint_24h
  FROM 
  cohorted c
  LEFT JOIN ratings r 
  ON r.ref_journey_id = c.ref_journey_id
)
,
future_trips AS (
  SELECT
    j.ref_customer_id,
    (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts
  FROM prod_etl_data.tbl_journey_master j
  WHERE j.journey_status IN (9,10)
    AND DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-09-01'
),

final AS (
  SELECT
    e.*,
    CASE WHEN EXISTS ( SELECT 1 FROM future_trips ft WHERE ft.ref_customer_id = e.ref_customer_id
          AND ft.journey_ts > e.journey_ts
          AND ft.journey_ts <= e.journey_ts + INTERVAL '30 day'
      )
      THEN 1 ELSE 0
    END AS retained_30d
  FROM enriched e
)
-- select * from final;

, filtered_jrny AS
(
select 
a.journey_id
,a.journey_dt
, a.ref_customer_id 
, a.price_bucket
,a.price_error
,a.actual_fee_bkt
, a.drop_dist_m
,a.estimate_total_fee
, a.actual_total_fee
,b.lifetime_completed_trips
,b.window_trip_index
,b.prior_completed_trips
,b.experience_cohort
,b.rating
,b.low_rating_lt3
,b.low_rating_lt4
,b.has_complaint_24h
,b.retained_30d
from bucketed as a
 left join 
 final as b
  on a.journey_id =b.journey_id
WHERE drop_dist_m <= 50   --- filter for drop distance parity
AND estimate_total_fee > 0
and journey_dt <'2025-11-01'
)

-- select * from filtered_jrny;

-- select 
-- distinct 
-- journey_dt
-- from filtered_jrny where experience_cohort is null;
-- select count(*), count(distinct journey_id) from filtered_jrny;

-- OYLBDCQV74508	66e0255766753ef8e352b9a9	0–5%	22.96756587027492	93	94.5	5	2	6	6–10	1	1	1	1	0	
,cust_level AS 
(
    SELECT
        ref_customer_id,
        price_bucket,
        experience_cohort,
        actual_fee_bkt,

        MAX(retained_30d)             AS retained_30d_flag,
        MAX(low_rating_lt3)           AS low_rating_lt3_flag,
        MAX(low_rating_lt4)           AS low_rating_lt4_flag,
        MAX(has_complaint_24h)        AS complaint_flag

    FROM filtered_jrny
    GROUP BY 1,2,3,4
)
-- select * from cust_level;

SELECT
    price_bucket,
    experience_cohort,
    actual_fee_bkt,
    COUNT(*) AS total_customers,
    -- true customer-level percentages
    SUM(retained_30d_flag::float)   AS pct_retained_30d,
    SUM(low_rating_lt3_flag::float) AS pct_rating_lt3,
    SUM(low_rating_lt4_flag::float) AS pct_rating_lt4,
    SUM(complaint_flag::float)      AS pct_with_complaint_24h

FROM cust_level
GROUP BY 1,2,3
ORDER BY price_bucket, experience_cohort;





SELECT
  price_bucket,
  experience_cohort,
  COUNT(*)                                  AS mispriced_trips,
  AVG(price_error)                          AS avg_price_error,
  AVG(retained_30d::FLOAT)                  AS retention_30d,
  AVG(low_rating_lt3::FLOAT)                AS pct_rating_lt3,
  AVG(low_rating_lt4::FLOAT)                AS pct_rating_lt4,
  AVG(has_complaint_24h::FLOAT)             AS pct_with_complaint_24h
FROM filtered_jrny
GROUP BY 1,2
ORDER BY price_bucket, experience_cohort;



