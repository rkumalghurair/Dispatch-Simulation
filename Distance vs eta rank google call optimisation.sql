with jrny_details as
(
select 
journey_id
,date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') as dt
,ref_driver_id
,pickup_latitude
,pickup_longitude
,case when ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
            then 'Taxi' else 'Limo' end as vehicle_cat
,journey_status
from 
prod_etl_data.tbl_journey_master  
where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-11-01'
and date ( (journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <=  '2025-11-24'
-- and ref_driver_id is not null
and ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba') --taxi details
)


-- select * from  jrny_details ;

,base as
(
SELECT
distinct
  journey_id as journeyid,
  driver_id,
  attempt,
  dispatch_order,
  distance_from_pickup,
  estimate_eta  as estimatedeta,
  estimate_distance,
  is_dispatched,
  attempt,
  latitude, 
  longitude,
  date (("timestamp"::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') as dt,
  ("timestamp"::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as tm,
  extract (hour from dt)as hr
FROM 
prod_etl_data.tbl_journey_history as a 
WHERE 
date ((timestamp::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-11-01'
and date ((timestamp::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <'2025-11-24'
and estimate_eta is not null
QUALIFY attempt = MAX(attempt) OVER (PARTITION BY journey_id)
)
,joined as(
select 
a.*
,b.pickup_latitude
,b.pickup_longitude
,ST_DistanceSphere(
    ST_MakePoint(pickup_longitude, pickup_latitude),
    ST_MakePoint(longitude, latitude)) AS drop_dist_m


,b.ref_driver_id as b_ref_driver_id
 from base as a
 inner join 
 jrny_details  as b 
  on a.journeyid =b.journey_id
)
,ranked as(
select *, CASE WHEN eta_rank = dist_rank THEN 1 ELSE 0 END AS ranks_match
from(
select 
journeyid 
,dt
,tm
,driver_id
,estimatedeta
,estimate_distance
-- ,latitude
-- ,longitude
-- ,pickup_latitude
-- ,pickup_longitude
,drop_dist_m
,ROW_NUMBER() OVER (PARTITION BY journeyid ORDER BY estimatedeta, estimate_distance ASC) AS eta_rank
,ROW_NUMBER() OVER (PARTITION BY journeyid ORDER BY drop_dist_m ASC) AS dist_rank
from 
joined
)
)
, eta_best AS 
(
SELECT journeyid, driver_id  as eta_best_driver
FROM ranked
WHERE eta_rank = 1
)
-- select count(distinct journeyid ), count(*) from eta_best; -- 216276

,match_check AS (
    SELECT
        r.journeyid,
        r.driver_id,
        r.dist_rank,
        eb.eta_best_driver,

        CASE WHEN r.driver_id = eb.eta_best_driver AND r.dist_rank <= 3 THEN 1 ELSE 0 END AS eta1_in_top3,
        CASE WHEN r.driver_id = eb.eta_best_driver AND r.dist_rank <= 4 THEN 1 ELSE 0 END AS eta1_in_top4,
        CASE WHEN r.driver_id = eb.eta_best_driver AND r.dist_rank <= 5 THEN 1 ELSE 0 END AS eta1_in_top5
    FROM ranked r
    LEFT JOIN eta_best eb
        ON r.journeyid = eb.journeyid
)
-- select * from match_check  where  journeyid='OAAACDYZ15252';
SELECT
    COUNT(DISTINCT journeyid) AS total_journeys,
    SUM(eta1_in_top3) * 1.0 / COUNT(DISTINCT journeyid) AS pct_eta1_in_top3,
    SUM(eta1_in_top4) * 1.0 / COUNT(DISTINCT journeyid) AS pct_eta1_in_top4,
    SUM(eta1_in_top5) * 1.0 / COUNT(DISTINCT journeyid) AS pct_eta1_in_top5
FROM match_check;





-- select * from ranked  where  journeyid='OAAACDYZ15252';

