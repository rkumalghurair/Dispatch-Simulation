with jrny_details as
(
select 
journey_id
,date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') as dte
,ref_driver_id
,case when ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
            then 'Taxi' else 'Limo' end as vehicle_cat
,journey_status
from 
prod_etl_data.tbl_journey_master  
where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-10-20'
and date ( (journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <=  '2025-11-03'
and ref_driver_id is not null
and ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
)


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
  "timestamp" as dt,
  date(dt)as dat,
  extract (hour from dt)as hr,
  b.final_score
FROM prod_etl_data.tbl_journey_history as a 
left join 
(
  SELECT
  distinct 
  ref_driver_id,
  final_score,
  date(query_run_timestamp) as query_run_date
FROM prod_etl_temp.taxi_driver_scores
where date(query_run_timestamp) ='2025-10-27'
) as b
 on a.driver_id =b.ref_Driver_id 
WHERE 
timestamp>= '2025-10-20'
and timestamp <='2025-11-03'
and estimate_eta is not null
QUALIFY attempt = MAX(attempt) OVER (PARTITION BY journey_id)
)
-- select 
-- dte, count(*)cnt
-- from(
-- select jrny.journey_id, dte, base.journeyid from jrny_details jrny left join base on  jrny.journey_id =base.journeyid
-- ) where  journeyid is null group by 1 order by dte;
-- select count(distinct journeyid) from base; -- 158000
-- select count(*), count(distinct concat (journeyid, driver_id)) from base; -- 894719
-- select * from base;

,eta_scaled_1 AS 
(
  SELECT
    b.*,
    MIN(estimatedeta::float8) OVER (PARTITION BY journeyid) AS min_eta,
    MAX(estimatedeta::float8) OVER (PARTITION BY journeyid) AS max_eta
from base as b
)
,eta_scaled as
(
select 
* 
,case when max_eta = min_eta then 1 else (max_eta - estimatedeta ::float8 )/ (max_eta - min_eta) end as eta_score
from eta_scaled_1
)


,summary as
(
select 
score.*
, jrny_details.ref_driver_id as drivr_assigned
,jrny_details.journey_id
,vehicle_cat
,journey_status
,( 0.8* eta_score + 0.2* final_Score ) as dispatch_score
from 
eta_scaled as score 
left join 
jrny_details
on score.journeyid =jrny_details.journey_id
)

,blended as
(
select 
* 
,ROW_NUMBER() OVER (
      PARTITION BY journeyid ORDER BY estimatedeta ASC, estimate_distance ASC) as eta_rank

,ROW_NUMBER() OVER (
      PARTITION BY journeyid
      ORDER BY
       dispatch_score DESC   ,  estimatedeta ASC    -- then shorter distance
        ) AS dispatch_rank

from summary r
where vehicle_cat='Taxi'
and estimatedeta is not null
) 
-- select * from blended where journeyid='OADCRSUJ49957';
, outlier_cases as 
(
select * from 
(
  select 
  * 
  ,
  case when (dispatch_rank =1   and eta_rank=1) then 'ETA_rank' 
  when (dispatch_rank =1 and eta_rank >1 ) then 'Driver_Score_priortised'
  when (eta_rank =1 and dispatch_rank >1 ) then 'ETA_priortised'
  end as flag
  from blended 
  where    
  driver_id=drivr_assigned
  and drivr_assigned is not null
) 
where dt >= '2025-10-29'and  ( flag is null or flag in( 'ETA_priortised'))

)
select distinct journey_id from outlier_cases;

select 
dat
,dispatch_rank
, count(distinct journey_id)as jrny_cnt
 from blended 
where    
driver_id=drivr_assigned
and drivr_assigned is not null
group by 1,2;



select 
dat 
,case 
when (dispatch_rank =1   and eta_rank=1) then 'ETA_rank_1' 
when (dispatch_rank =1  and eta_rank >1 ) then 'Driver_Score_priortised'
else 'Others' end as flg,
count(distinct journey_id) as jnry_cnt,
count(distinct case when journey_status in (9,10) then journey_id end )as completed_jrny,
count(distinct case when journey_status in (13) then journey_id end )as customer_canclled,
count(distinct case when journey_status in (14) then journey_id end )as driver_canclled,
avg(estimatedeta)as avg_eta,
avg(final_score)final_score
-- ,PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY estimatedeta) as p10
-- ,PERCENTILE_CONT(0.20) WITHIN GROUP (ORDER BY estimatedeta) as p20
-- ,PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY estimatedeta) as p50
-- ,PERCENTILE_CONT(0.70) WITHIN GROUP (ORDER BY estimatedeta) as p70
-- ,PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY estimatedeta) as p90

from blended 
where    
driver_id=drivr_assigned
and drivr_assigned is not null
group by 1,2 
order by dat;







select 
-- date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') as dte,
case when b.final_score <= 0.2 then '0.less_then0.2'
when b.final_score <= 0.4 then '1.less_then0.4'
when b.final_score <= 0.6 then '2.less_then0.6'
when b.final_score <= 0.8 then '3.less_then0.8'
when b.final_score <= 1 then '4.less_then1' end as score,
count(distinct journey_id)as jrny_cnt
,count(distinct case when journey_status in (13) then journey_id end )customer_cancelled_jrny
,count(distinct case when journey_status in (14) then journey_id end  )driver_Cancelled_jrny
, count(case when journey_status in (9,10) then journey_id end )as completed_jrny
from 
prod_etl_data.tbl_journey_master  as a
left join 
(
  SELECT
  distinct 
  ref_driver_id,
  final_score,
  date(query_run_timestamp) as query_run_date
FROM prod_etl_temp.taxi_driver_scores
where date(query_run_timestamp) ='2025-10-27'
) as b
 on a.ref_driver_id =b.ref_Driver_id 


where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-10-28'
and date ( (journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') <=  '2025-11-02'
and a.ref_driver_id is not null
and a.ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
 group by 1;
