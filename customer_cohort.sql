--  create table test_ric.temp_customer_cohort as
--  drop table test_ric.temp_customer_cohort

with customer_base as
(
SELECT 
usertype,
_id as ref_customer_id,
useruid as customer_id,
name,
countrycode,
mobilenumber,
createdat,
emailid,
case 
when countrycode like '%+971%' then 1 else 0 end as local_flag
FROM public.users 
where usertype IN (1)
and createdat >= '2025-02-01'
)
-- select count(*), count(distinct ref_customer_id) from customer_base; -- 305,821
,pickup_hood_mapping as
(
SELECT
a.journey_id
,a.customer_id
,a.pickup_location
,trim(lower(b.name)) as pickup_hood_name
,case when  
lower(trim(b.name)) like '%mall%' or lower(trim(b.name)) like '%city centre%' or lower(trim(b.name)) like '%burjuman%'  or lower(trim(b.name)) like '%dragon mart%'
or lower(trim(a.pickup_location))  like '%mall%' then 'Mall'
when lower(trim(b.name)) like '%airport%' or lower(trim(b.name)) like '%terminal%' then 'Airport'
when lower(trim(b.name)) like '%metro%' or lower(trim(a.pickup_location)) ILIKE '%metro%' then 'Metro'
when lower(trim(b.name)) like '%school%' or lower(trim(a.pickup_location)) ILIKE '%school%' then 'School'
when lower(trim(b.name)) like '%college%' or lower(trim(b.name)) like '%university%' or lower(trim(a.pickup_location)) ILIKE '%college%' 
or lower(trim(a.pickup_location)) ILIKE '%university%' then 'College'
else 'Others'
end as pickup_location_type_flag
FROM 
prod_etl_data.tbl_journey_master as a
LEFT JOIN prod_etl_temp.tbl_hood_kml_with_zone AS b
  ON ST_Contains(
       b.geom,
       ST_SetSRID(ST_Point(a.pickup_longitude, a.pickup_latitude), ST_SRID(b.geom))
     )
where  
DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-10-23' 
QUALIFY ROW_NUMBER() OVER ( PARTITION BY a.journey_id ORDER BY ST_Area(b.geom) ASC) = 1
)

,dropoff_hood_mapping AS
(
SELECT
a.journey_id
,a.customer_id
,a.pickup_location
,trim(lower(b.name)) as drop_hood_name
,case 
when  
lower(trim(b.name)) like '%mall%' or lower(trim(b.name)) like '%city centre%' or lower(trim(b.name)) like '%burjuman%'  or lower(trim(b.name)) like '%dragon mart%'
or lower(trim(a.drop_off_location))  like '%mall%' then 'Mall'

when lower(trim(b.name)) like '%airport%' or lower(trim(b.name)) like '%terminal%' then 'Airport'
when lower(trim(b.name)) like '%metro%' or lower(trim(a.drop_off_location)) ILIKE '%metro%' then 'Metro'
when lower(trim(b.name)) like '%school%' or lower(trim(a.drop_off_location)) ILIKE '%school%' then 'School'
when lower(trim(b.name)) like '%college%' or lower(trim(b.name)) like '%university%' or lower(trim(a.drop_off_location)) ILIKE '%college%' 
or lower(trim(a.drop_off_location)) ILIKE '%university%' then 'College'

else 'Others'
end as drop_location_type_flag
FROM 
prod_etl_data.tbl_journey_master as a
LEFT JOIN prod_etl_temp.tbl_hood_kml_with_zone AS b
  ON ST_Contains(
       b.geom,
       ST_SetSRID(ST_Point(a.actual_drop_off_longitude, a.actual_drop_off_latitude), ST_SRID(b.geom))
     )
where   
DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-10-23' 
QUALIFY ROW_NUMBER() OVER (PARTITION BY a.journey_id ORDER BY ST_Area(b.geom) ASC) = 1 
)
,journey_category as 
(
SELECT 
ref_customer_id
,count(distinct CASE WHEN ref_vehicle_category_id IN ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba') THEN 
journey_id end ) as Taxi_request
,count(distinct CASE WHEN ref_vehicle_category_id NOT IN ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba') THEN 
journey_id end ) as Limo_request

,count(distinct case when journey_type_flag='Mall_Goer' then journey_id end) as mall_trips
,count(distinct case when journey_type_flag='School_Route' then journey_id end) as school_trips
,count(distinct case when journey_type_flag='College_Route' then journey_id end) as college_trips
,count(distinct case when journey_type_flag='Airport_Traveler' then journey_id end) as airport_trips
,count(distinct case when journey_type_flag='Metro_Route' then journey_id end) as metro_trips
from
(
  select
     j.journey_id,
     j.ref_customer_id,
     j.ref_vehicle_category_id,
     p.pickup_location_type_flag,
     d.drop_location_type_flag,
     case 
        when p.pickup_location_type_flag='Mall' or d.drop_location_type_flag='Mall' then 'Mall_Goer'
        when p.pickup_location_type_flag='School' or d.drop_location_type_flag='School' then 'School_Route'
        when p.pickup_location_type_flag='College' or d.drop_location_type_flag='College' then 'College_Route'
        when (p.pickup_location_type_flag='Metro' or d.drop_location_type_flag='Metro') then 'Metro_Route'
        when (p.pickup_location_type_flag='Airport' or d.drop_location_type_flag='Airport') then 'Airport_Traveler'
        else 'General'
     end as journey_type_flag

  from prod_etl_data.tbl_journey_master as j
  left join pickup_hood_mapping p on j.journey_id = p.journey_id
  left join dropoff_hood_mapping d on j.journey_id = d.journey_id
 WHERE  DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-10-23' 
)
group by 1
)
,RFM as
(
select 
ref_customer_id
,date_part('week', max((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')) as last_week_active 
,count(distinct date_part('week', (journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')) as active_weeks --Active ≥8 of last 12 weeks → Regular CommutersActive ≤2 weeks → Occasional / Tourists
,min(DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))as first_jrny_date
,max(DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))as last_jrny_date
,count( distinct case when  journey_status IN (9,10) then journey_id end )as completed_jrny
,count( distinct case when  journey_status IN (13 )  then journey_id end )as user_cancelled_jrny
,count( distinct  journey_id  )as total_jrny_request
,avg (actual_distance)as avg_trip_distance
,avg(estimate_total_fee) as avg_estimated_fee
,avg (actual_total_fee) as avg_actual_fee


,sum(actual_total_fee)  as actual_total_fee
,sum(actual_discount_amount)as total_discount_amount
, sum(actual_total_fee-actual_discount_amount)as total_fare_after_discount

,stddev(actual_total_fee) as stddev_trip_cost
,count(distinct case when ref_promo_applied is not null then journey_id end )as jrny_request_with_promo
,count(distinct case when ref_promo_applied is  null then journey_id end )as jrny_request_without_promo
,avg(case when journey_status IN (9,10) then estimate_total_fee - actual_total_fee end) as avg_fee_diff

,count(distinct case when extract(hour from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')) between 6 and 10 then journey_id end) as morning_trips
,count(distinct case when extract(hour from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))  between 14 and 20 then journey_id end) as evening_trips
 ,count(distinct case when extract(hour from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))  in (21,22,23,0,1,2,3) then journey_id end) as night_trips
 ,count(distinct case when extract(dow from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))  in (0,6) then journey_id end ) as weekend_trips

,count(distinct case when extract(dow from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))  in (0,6) and  extract(hour from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'))  in (21,22,23,0,1,2,3) 
then journey_id end )
 as weekend_and_night_trips


from 
prod_etl_data.tbl_journey_master
WHERE 
DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-10-23'
group by 1
)

,percentile as
(
select 
ref_customer_id
,percentile_cont(0.9) within group (order by actual_total_fee) as p90_trip_cost
,percentile_cont(0.8) within group (order by actual_total_fee) as p80_trip_cost
,percentile_cont(0.5) within group (order by actual_total_fee) as p50_trip_cost
-- ,percentile_cont(0.8) within group (order by actual_distance) as p80_distance
from
prod_etl_data.tbl_journey_master
WHERE 
DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-10-23'
 group by 1
)
,hourly_pattern as 
(
  select
    ref_customer_id,
    extract(hour from ((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')) as hr_local,
    count(distinct journey_id) as hr_trip_cnt
  from prod_etl_data.tbl_journey_master
  where DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-10-23'
  group by 1,2
)
,summary as
(
select 
  cb.ref_customer_id,
  cb.customer_id,
  cb.name,
  cb.countrycode,
  cb.mobilenumber,
  cb.emailid,
  cb.local_flag,
  coalesce(r.actual_total_fee,0) as actual_total_fee,
  coalesce(r.total_fare_after_discount,0) as total_fare_after_discount,
  coalesce(r.total_discount_amount,0) as total_discount_amount,
case 
   when coalesce(r.total_jrny_request,0) = 0 then 'No_Request'
   when coalesce(r.jrny_request_with_promo,0)::decimal(18,6) 
        / nullif(r.total_jrny_request::decimal(18,6),0) >= 0.8 then 'Promo_Heavy_User'
   when coalesce(r.jrny_request_with_promo,0)::decimal(18,6) 
        / nullif(r.total_jrny_request::decimal(18,6),0) between 0.3 and 0.8 then 'Promo_Balanced_User'
   when coalesce(r.jrny_request_with_promo,0)::decimal(18,6) 
        / nullif(r.total_jrny_request::decimal(18,6),0) < 0.3 
        and coalesce(r.jrny_request_with_promo,0) > 0 then 'Occasional_Promo_User'
   when coalesce(r.jrny_request_with_promo,0) = 0 
        and coalesce(r.total_jrny_request,0) > 0 then 'Non_Promo_User'
   else 'Unknown'
 end as promo_usage_segment,

  -- Journey performance metrics
  coalesce(r.completed_jrny,0) as completed_jrny,
  coalesce(r.total_jrny_request,0) as total_jrny_request,
  coalesce(r.user_cancelled_jrny,0) as cancelled_jrny,
  coalesce(r.active_weeks,0) as active_weeks,
  coalesce(Taxi_request,0) as Taxi_request,
  coalesce(Limo_request,0) as limo_request,
  r.first_jrny_date,
  r.last_jrny_date,
  (dateadd(hour, 4, getdate())::date - r.last_jrny_date) as recency_days
   ,round(coalesce(r.user_cancelled_jrny::numeric / nullif(r.total_jrny_request,0),0),3) as cancellation_ratio,
    coalesce(r.avg_trip_distance,0) as avg_trip_distance,
     coalesce(r.avg_fee_diff,0) as avg_fee_diff,

  case 
    when r.first_jrny_date is null then 'Never_Active'
    when (dateadd(hour, 4, getdate())::date - r.last_jrny_date) <= 7  then 'Active_This_Week'
        when (dateadd(hour, 4, getdate())::date - r.last_jrny_date) <= 14  then 'Active_Past2_Week'
    when (dateadd(hour, 4, getdate())::date - r.last_jrny_date) <= 30 then 'Active_This_Month'
    when (dateadd(hour, 4, getdate())::date - r.last_jrny_date) <= 90 then 'Active_Last_3_Months'
    when (dateadd(hour, 4, getdate())::date - r.last_jrny_date) <= 180 then 'Dormant_3_6M'
    else 'Dormant_6Mplus'
  end as recency_flag,
  
  coalesce(r.avg_actual_fee,0) as avg_actual_fee,
  coalesce(p.p90_trip_cost,0) as p90_trip_cost,
  coalesce(p.p80_trip_cost,0) as p80_trip_cost,
  coalesce(p.p50_trip_cost,0) as median_trip_cost,
  -- coalesce(r.avg_trip_distance,0)as avg_trip_distance,
  -- Journey category counts
  coalesce(j.mall_trips,0) as mall_trips,
  coalesce(j.school_trips,0) as school_trips,
  coalesce(j.college_trips,0) as college_trips,
  coalesce(j.metro_trips,0) as metro_trips,
  coalesce(j.airport_trips,0) as airport_trips,
  -- Temporal behavior
  coalesce(r.morning_trips,0) as morning_trips,
  coalesce(r.evening_trips,0) as evening_trips,
  coalesce(r.night_trips,0) as night_trips,
  coalesce(r.weekend_trips,0) as weekend_trips,
  -- Derived Flags
  case 
    when r.total_jrny_request = 0 then 'Acquired_No_Trip'
     when r.total_jrny_request > 0 and completed_jrny =0 then 'Attempted_but_0_fulfilled'
    when r.completed_jrny = 1 then 'Single_Completed_Trip_User'
    when r.completed_jrny between 2 and 9 then 'Casual_User'
    when r.completed_jrny >= 10 then 'Power_User'
    else 'Unknown'
  end as lifecycle_segment,



case
    when ((r.morning_trips + r.evening_trips)::numeric / nullif(r.total_jrny_request,0)) >= 0.6
         and (r.weekend_trips::numeric / nullif(r.total_jrny_request,0)) < 0.4
         then 'Commuter'
    when (r.weekend_trips::numeric / nullif(r.total_jrny_request,0)) >= 0.6
         then 'Weekend'
    else 'General_Purpose'
end as primary_user_type,


CASE 
    
    WHEN ((r.morning_trips + r.evening_trips)::numeric / NULLIF(r.total_jrny_request,0)) >= 0.6
         AND (r.weekend_trips::numeric / NULLIF(r.total_jrny_request,0)) < 0.4 
    THEN 
        CASE 
            WHEN (j.metro_trips::numeric / NULLIF(r.total_jrny_request,0)) >= 0.5 THEN 'Metro_User'
            WHEN (j.school_trips::numeric / NULLIF(r.total_jrny_request,0)) >= 0.5 THEN 'School_User'
            WHEN (j.college_trips::numeric / NULLIF(r.total_jrny_request,0)) >= 0.5 THEN 'College_User'
            ELSE 'Othercommuters'
        END
    WHEN (r.weekend_trips::numeric / NULLIF(r.total_jrny_request,0)) >= 0.6 
    THEN 
        CASE 
            WHEN (j.mall_trips::numeric / NULLIF(r.total_jrny_request,0)) >= 0.5 THEN 'Mall_User'
            WHEN (r.night_trips::numeric / NULLIF(r.total_jrny_request,0)) >= 0.5 THEN 'Night_User'
            ELSE 'Daytime_Leisure_User'
        END

    ELSE 'General_Purpose'
END AS sub_user_type
 
--   case 
--     when coalesce(r.completed_jrny,0)=0 then 'Inactive'
--     when ((r.morning_trips + r.evening_trips)::numeric / nullif(r.total_jrny_request,0)) >= 0.6 
--     and (r.weekend_trips::numeric / nullif(r.total_jrny_request,0)) < 0.3 then 'Commuter'
--     when (j.school_trips::numeric / nullif(r.total_jrny_request,0)) >= 0.7 then 'School_trips'
--     when (j.college_trips::numeric / nullif(r.total_jrny_request,0)) >= 0.7 then 'college_trips'
--     when (j.metro_trips::numeric / nullif(r.total_jrny_request,0)) >= 0.6 then 'Metro_User'
--     when (j.airport_trips::numeric / nullif(r.total_jrny_request,0)) >= 0.6 then 'Frequent_Airport_Traveler'
--     when (j.mall_trips::numeric / nullif(r.total_jrny_request,0)) >= 0.6 then 'Mall_Goer'
--     when ((r.weekend_trips)::numeric / nullif(r.total_jrny_request,0)) >= 0.70 then 'Weekend_trips'
--     when ((r.night_trips)::numeric / nullif(r.total_jrny_request,0)) >= 0.70 then 'night_trips'
--     -- when ((r.weekend_and_night_trips)::numeric / nullif(r.completed_jrny,0)) >= 0.6 then 'weekend_and_night_trips'
--   else 'General_User'
-- end as behavioral_segment
,case 
   when coalesce(p.p80_trip_cost,0) >= 100 then 'High_Value'
   when coalesce(p.p80_trip_cost,0) between 50 and 100 then 'Mid_Value'
   when coalesce(p.p80_trip_cost,0) < 50 and coalesce(r.completed_jrny,0) > 0 then 'Low_Value'
   else 'No_Value'
 end as fare_tier_segment

,case 
   when coalesce(r.avg_trip_distance,0) >= 25 then 'Long_Haul_User'
   when coalesce(r.avg_trip_distance,0) between 8 and 25 then 'Mid_Range_User'
   when coalesce(r.avg_trip_distance,0) < 8 and coalesce(r.avg_trip_distance,0) > 0 then 'Short_Haul_User'
   else 'No_Journey'
 end as distance_segment

,case 
   when r.active_weeks >= 8  then 'Consistent_Weekly_User'
   when r.active_weeks between 4 and 7 then 'Occasional_User'
   when r.active_weeks between 1 and 3 then 'Infrequent_User'
   else 'Inactive'
 end as consistency_segment

,case
  when coalesce(r.total_jrny_request,0) = 0 then 'No_Request'
  when coalesce(taxi_request,0)::decimal(18,6) 
       / nullif(r.total_jrny_request::decimal(18,6), 0) >= 0.7 then 'Taxi_User'
  when coalesce(limo_request,0)::decimal(18,6) 
       / nullif(r.total_jrny_request::decimal(18,6), 0) >= 0.7 then 'Limo_User'
  else 'Others'
end as service_mix_segment
from customer_base cb
left join RFM r on cb.ref_customer_id = r.ref_customer_id
left join journey_category j on cb.ref_customer_id = j.ref_customer_id
left join percentile p on cb.ref_customer_id = p.ref_customer_id

)

select * 
, 
case
when coalesce(total_jrny_request,0) = 0 then 'No_Request'
when cancellation_ratio > 0.4 then 'High_Canceller'
when cancellation_ratio between 0.2 and 0.4 then 'Moderate_Canceller'
when cancellation_ratio < 0.2 then 'Low_Canceller'
else 'No_Data'
end as cancellation_behavior_segment
from summary;


