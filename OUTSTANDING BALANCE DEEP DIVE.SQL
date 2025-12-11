with device_details as
(
select 
* from
    (
        select 
        _id
        , refuserid
        , devices
        ,json_extract_path_text(devices, 'devices', '0', 'deviceId') AS device_id
        ,json_extract_path_text(devices, 'devices', '0', 'deviceName') AS device_name
        , json_extract_path_text(devices, 'devices', '0', 'deviceOs')   AS device_os      
        ,updatedat
        ,ROW_NUMBER() OVER (PARTITION BY refuserid ORDER BY updatedat DESC) as rn
        from  "public"."userdevices" 
    ) 
 where rn=1 -- there are 2 cases where same refuserid X device_id has multiple entry so removing the duplicates 
)

, jrny_details as
(
SELECT
journey_id
,customer_id
,ref_customer_id
,journey_status
,ref_promo_applied
,CASE WHEN ref_vehicle_category_id IN ('65d4bce6d30d222a7c7921b9','65d4bce6d30d222a7c7921ba') THEN 'Taxi' ELSE 'Limo' END AS vehicle_cat
,(journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts
FROM 
prod_etl_data.tbl_journey_master as a
where  
DATE((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < current_Date

)

,joined as
(
select 
a.* 
,b.device_id
,b.device_name
,b.device_os
from 
jrny_details as a 
left join 
device_details  as b 
on a.ref_customer_id =b.refuserid
where b.device_id is not NULL
)

-- select count(*), count(distinct journey_id) from joined; -- 2171772
,outstanding_balance as
(
select
customer_id
, type
, sum(balance)as total_outstanding_balance
from  "public"."wallet"
where balance > 1
and trim(upper(type)) ='OUTSTANDING'
group by 1,2
 )


-- select sum(balance) from outstanding_balance; -- 106,836
-- select count(*), count(distinct  (customer_id)) from outstanding_balance; ---2939
-- select distinct customer_id , ref_customer_id ,count(distinct journey_id) journey_id, sum(total_outstanding_balance)total_outstanding_balance
-- from balance_added where device_id ='1F1A000A-C66F-41B0-AB19-A564B92872A3' group by 1,2
 ,fraud_customer_1 as
 (
 select
 device_id
 ,device_os
 ,ref_customer_id
 ,customer_id
 , count(distinct journey_id)as total_jrny_request
  , count(distinct case when vehicle_cat ='Limo' then journey_id end)as limo_jrny_request
 , count(case when ref_promo_applied is not null then  journey_id end )as jrny_with_promo
 , count(case when journey_status in (9,10)  then  journey_id end )as completed_jrny
 ,count(case when journey_status in (13) then journey_id end )as customer_cancelled_jrny
from joined 
where device_id is not null 
group by 1,2  ,3,4
)

,fraud_customer as
(
select 
 device_id
 ,count(distinct ref_customer_id ) as customer_cnt
 ,sum(total_jrny_request)total_jrny_request
 ,sum(limo_jrny_request)as limo_jrny_request
 ,sum(jrny_with_promo)jrny_with_promo
 ,sum(completed_jrny)completed_jrny
 ,sum(customer_cancelled_jrny)as customer_cancelled_jrny
 ,coalesce(sum(total_outstanding_balance),0)total_outstanding_balance

from fraud_customer_1 as a 
left join outstanding_balance as b 
on a.ref_Customer_id =b.customer_id
 where  1=1
--  device_id='1F1A000A-C66F-41B0-AB19-A564B92872A3'
group by 1 
order by total_outstanding_balance desc
)

--  where customer_cnt >=3 and device_id <>'';

,summary as
(
 SELECT
 device_id,
 customer_cnt,
 total_jrny_request,
 limo_jrny_request,
 jrny_with_promo,
 completed_jrny,
 customer_cancelled_jrny,
 total_outstanding_balance,
 (jrny_with_promo::float / NULLIF(total_jrny_request,0)) AS promo_ratio,
  (limo_jrny_request::float / NULLIF(total_jrny_request,0)) AS limo_ratio,
 (customer_cancelled_jrny::float / NULLIF(total_jrny_request,0)) AS customer_cancel_ratio
FROM fraud_customer
where  device_id <>''
)

-- select sum(jrny_with_promo) from summary where customer_cnt =3 and device_id <>'' ;
select sum(customer_cnt), count(distinct device_id), sum(total_outstanding_balance) from summary 
WHERE customer_cnt =3 and device_id <>'' and promo_ratio >=0.75 ;

1929	643	4088.3
