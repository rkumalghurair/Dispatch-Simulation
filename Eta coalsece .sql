


with amplitude_eta_time as
(
SELECT
user_id
,event_time
,(event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as local_event_time
,event_type
,json_extract_path_text(json_serialize(event_properties) , 'Journey_id') AS journey_id
,json_extract_path_text(json_serialize(event_properties) , 'ETA') AS eta
,REGEXP_REPLACE(
  json_extract_path_text(json_serialize(event_properties), 'ETA'),
  '[^0-9.]',
  ''
)::numeric AS eta_numeric
	
FROM amplitude_customer_app.events
WHERE 
event_type = 'eta_refresh'
and date((event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-11-06' and date((event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < '2025-11-13'
)

, jrny_details as(
select 
a.journey_id
,a.ref_journey_id
,a.ref_customer_id
,a.ref_vehicle_category_id
,a.customer_id
,(journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as local_journey_created_time
,date((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') as local_created_date
,EXTRACT(HOUR FROM (a.journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')as local_booking_hr
,TO_CHAR((a.journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai', 'Day')as weekday
,a.journey_Status_desc
,a.journey_status

,a.first_eta 
,a.dispatch_eta
,a.dispatch_eta_sec
,a.accepted_ride_timestamp
,a.on_route_timestamp
,a.arrived_at_pickup_timestamp
,a.journey_cancel_timestamp
,(a.journey_cancel_timestamp::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as local_cancel_time
,case when a.ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
then 'Taxi' else 'Limo' end as vehicle_cat
 from 
 prod_etl_data.tbl_journey_master  as a 
where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-11-06'
and date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' )< '2025-11-13'
and  driver_id is not null
and journey_status in (13)
and a.ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba') --taxi trips
-- and journey_type=1
)
-- select count(distinct journey_id) from jrny_details --  19271
,latest_eta AS 
(
  SELECT
    j.journey_id,
    j.local_cancel_time,
	 j.local_journey_created_time,
    e.local_event_time,
    e.user_id,
	 e.journey_id as e_jrny,
	 e.eta_numeric,
	
    ROW_NUMBER() OVER (PARTITION BY j.journey_id ORDER BY e.local_event_time DESC) AS rn
  FROM jrny_details j
  LEFT JOIN amplitude_eta_time e
    ON j.journey_id = e.journey_id
   AND e.local_event_time <= j.local_cancel_time
   -- QUALIFY ROW_NUMBER() OVER (PARTITION BY j.journey_id ORDER BY e.local_event_time DESC) =1
)

,all_details as
(
SELECT 
  j.journey_id,
  j.first_eta ,
  j.dispatch_eta,
  l.user_id,
  l.local_event_time AS latest_eta_time,
  j.local_cancel_time,
  l.eta_numeric,
  j.local_journey_created_time,
  DATEDIFF(second, j.local_journey_created_time, j.local_cancel_time) AS time_to_cancel_sec,
 DATEDIFF(second, l.local_event_time, j.local_cancel_time) AS seconds_before_cancel,
  DATEDIFF(second,  j.local_journey_created_time, l.local_cancel_time) AS cancel_time
FROM 
jrny_details j 
LEFT JOIN latest_eta l
ON j.journey_id = l.journey_id
WHERE l.rn = 1
)


select 

first_eta -(ata_proxy)as diff
,count(journey_id) from
(
select *
,round ((eta_numeric )  +(cancel_time/60),0) as ata_proxy

 from all_details
) 
--  where user_id='CUS_SBURE45703'
 group by 1;
