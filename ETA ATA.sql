with jrny_detail AS(
select 
a.journey_id
,a.ref_journey_id
,a.ref_customer_id
,a.ref_driver_id
,a.driver_id
,a.ref_vehicle_category_id
,a.pickup_latitude
,a.pickup_longitude
,a.customer_id
,(journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as local_journey_created_time
,date((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') as local_created_date
,EXTRACT(HOUR FROM (a.journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai')as local_booking_hr
,TO_CHAR((a.journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai', 'Day')as weekday
,a.journey_Status_desc
,a.journey_status
,a.actual_total_fee
,a.estimate_total_fee
,a.first_eta 
,a.dispatch_eta
,a.dispatch_eta_sec
,a.accepted_ride_timestamp
,a.on_route_timestamp
,a.arrived_at_pickup_timestamp
,a.journey_cancel_timestamp
,a.journey_completed_timestamp
,case when a.ref_vehicle_category_id in ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba')
then 'Taxi' else 'Limo' end as vehicle_cat
 from 
 prod_etl_data.tbl_journey_master  as a 
where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-10-15'
and date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' )< '2025-11-13'
and  driver_id is not null
-- and journey_type=1

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
from jrny_detail
)
,base_3 as
(
select 
*
,DATEDIFF(second, journey_accepted_time, arrived_at_pickup_timestamp)as ata
,DATEDIFF(seconds, journey_accepted_time, journey_cancel_timestamp)as cancel_time_sec
,DATEDIFF (minute, arrived_at_pickup_timestamp, journey_completed_timestamp)as jrny_time
from base_2
)
select * from base_3;
