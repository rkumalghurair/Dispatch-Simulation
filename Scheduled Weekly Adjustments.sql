-- Step 1 : Purge anything older than 55 weeks
-- DELETE FROM prod_etl_data.Weekly_Adjustments_scheduled where  pickup_date< (DATE_TRUNC('week', (CURRENT_DATE AT TIME ZONE 'Asia/Dubai')) - INTERVAL '55 weeks')::date;

-- Step 2 : 
INSERT INTO prod_etl_data.Weekly_Adjustments_scheduled

-- create table prod_etl_data.Weekly_Adjustments_scheduled as
with adjusted_base AS
(
  select 
  journeyid
  ,sum(adjustmentamount) as adjustmentamount
  from
  (
    select distinct *
    from 
    public.journeyadjustment 
    where 
    lower(trim(adjustmentapplicability )) in ('both', 'driver')
  	and (createdat AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Dubai') >= (DATE_TRUNC('week', (CURRENT_DATE AT TIME ZONE 'Asia/Dubai')) - INTERVAL '7 days')::date 
    and (createdat AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Dubai') <  (DATE_TRUNC('week', (CURRENT_DATE AT TIME ZONE 'Asia/Dubai')))::date 
  
  )
  group by 1
)

,base as(
SELECT
  jm.journey_id,
  jm.Transport_Authority_job_id,
  jm.customer_name,
  (jm.pickup_time::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS pickup_time,
  -- jm.pickup_time,
  jm.pickup_zone_name,
  jm.drop_off_zone_name,
  jm.actual_drop_off_time AS drop_off_time,
  jm.actual_distance AS trip_distance,
  jm.notes AS remarks,
  jm.supplier_name,
  jm.actual_base_fee AS base_fare,
  jm.actual_extras_fee AS extras,
  jm.actual_salik_fee AS salik,
  jm.actual_interemirate_fee,
  jm.actual_peak_multiplier,

  jm.actual_parking_fee,
  jm.actual_tip_amount AS tips,
  jm.actual_discount_amount AS discount,
  jm.promo_code_description AS promotion,
  jm.actual_total_fee AS total_fare,

  
  jm.actual_commission AS commission,
  jm.actual_booking_extra_fee,
  jm.ref_vehicle_id AS vehicle_id,
  jm.driver_id,
  jm.driver_name,
  jm.journey_status_desc AS trip_status,
  jm.payment_description AS payment_mode,
  jm.updated_at AS journey_updated_at,
  jm.journey_id AS invoiceId,
  jm.customer_id AS passengerRef,
  jm.actual_distance,
  jm.pickup_location,
  jm.drop_off_location,
  jm.pickup_latitude,
  jm.pickup_longitude,

  jm.actual_drop_off_latitude,
  jm.actual_drop_off_longitude,
  jm.actual_total_cancellation_fee,
  inst.actual_payment_instrument,

  DATEDIFF(minute, jm.arrived_at_pickup_timestamp, jm.actual_drop_off_time) AS actual_time,

  driver.countrycode AS countryCode,
  CAST(driver.mobilenumber AS BIGINT) AS mobileNumber,
  'Dubai' AS REGION,
  driver.meterid,
  EXTRACT(
    EPOCH
    FROM
      (
        jm.journey_completed_timestamp - jm.journey_start_timestamp
      )
  ) / 60 AS tripDuration,
  jm.actual_total_cancellation_fee AS additionalFare,
  jm.actual_total_ontrip_wait_charge + jm.actual_total_pretrip_wait_fee AS waitingFare,
  CASE
    WHEN jm.vehicle_category_type = 1 THEN 'Y'
    ELSE 'N'
  END AS dispatchfeeRta,
  jm.actual_rta_fee AS dispatchFeeAmount,
  jm.actual_is_adjusted AS refunded,
  ja.adjustmentamount ,
  jm.actual_total_fee AS netFare,
  (
    jm.actual_total_fee + jm.actual_tip_amount - jm.actual_commission - jm.actual_commission_vat + ja.adjustmentamount
  ) AS franchise,
  jm.actual_total_fee AS invoiceAmount,
  
  CASE
    WHEN jm.vehicle_category_type = 1 THEN 'LIMO'
    ELSE 'TAXI'
  END AS vehicleType,
  
  jm.journey_completed_timestamp AS invoiceDate
FROM
  prod_etl_data.tbl_journey_master jm

LEFT JOIN (select distinct useruid,mobilenumber, countrycode from public.users )usr
   ON jm.customer_id = usr.useruid

LEFT JOIN (select distinct useruid,mobilenumber,meterid, countrycode from public.users  )driver
on jm.driver_id =driver.useruid

  LEFT JOIN 
  adjusted_base ja
ON jm.journey_id = ja.journeyid

LEFT JOIN  
(select distinct journey_id , actual_payment_instrument from prod_etl_data.tbl_actual_fare_metrics ) as inst
on jm.journey_id =inst.journey_id

WHERE
1=1
And ref_driver_id is not null
AND (jm.pickup_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Dubai')  >= (DATE_TRUNC('week', (CURRENT_DATE AT TIME ZONE 'Asia/Dubai')) - INTERVAL '7 days')::date 
AND (jm.pickup_time AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Dubai')  <  (DATE_TRUNC('week', (CURRENT_DATE AT TIME ZONE 'Asia/Dubai')))::date
)


, base_2 as (
select 
journey_id
,trip_status
,payment_mode as payment_method
,vehicleType as car_category
,supplier_name
,case when trip_status in ('PAYMENT_RECEIVED','JOURNEY_COMPLETED') then 'Completed' else 'Cancelled' end as final_journey_status
,driver_id
,driver_name
,mobilenumber
,meterid
,Transport_Authority_job_id 
,actual_distance
,actual_time
,pickup_location
,drop_off_location
,date(pickup_time) as pickup_date
,pickup_time

,pickup_latitude
,pickup_longitude

,date( drop_off_time) as dropoff_date
,drop_off_time
,actual_drop_off_latitude
,actual_drop_off_longitude
,salik
,discount
,actual_interemirate_fee
,actual_total_cancellation_fee
,actual_parking_fee
,commission
,tips
,total_fare
,actual_peak_multiplier
,adjustmentamount  

,actual_booking_extra_fee 


,case when trim(upper(payment_method)) like '%CASH%' then COALESCE(total_fare, 0) + COALESCE(adjustmentamount, 0) - coalesce(discount,0)  else 0 end as "Cash collected" -- only when customer hasnt paid or has not taken ride while it shows driver has calculated , or extra fare computation cases so actual cash collected is adjusted

,actual_payment_instrument
,dispatchFeeAmount


,CASE
WHEN TRIM(UPPER(payment_method)) LIKE '%CASH%' AND vehicleType = 'LIMO' 
THEN 
(
        COALESCE(total_fare, 0) 
        - COALESCE(commission, 0) 
		-coalesce(0.05*commission ,0)
        + COALESCE(adjustmentamount, 0) 
        - COALESCE(dispatchFeeAmount, 0)
		
    )
WHEN TRIM(UPPER(payment_method)) NOT  LIKE '%CASH%' AND vehicleType = 'LIMO' 
THEN 
( 
     COALESCE(total_fare, 0) 
        - COALESCE(commission, 0) 
		-coalesce(0.05*commission ,0)
        + COALESCE(adjustmentamount, 0) * (1 - COALESCE(commission, 0) / NULLIF(total_fare, 0))
    )

ELSE ( 
     COALESCE(total_fare, 0) 
        - COALESCE(commission, 0) 
        + COALESCE(adjustmentamount, 0) * (1 - COALESCE(commission, 0) / NULLIF(total_fare, 0))
    ) end AS netFare




from base
)



select 
journey_id
,trip_status
,payment_method
,car_category
,supplier_name
,final_journey_status
,driver_id
,driver_name
,mobilenumber
,meterid
,transport_authority_job_id
,actual_distance
,actual_time
,pickup_location
,drop_off_location
,pickup_date
,pickup_time
,pickup_latitude
,pickup_longitude
,dropoff_date
,drop_off_time
,actual_drop_off_latitude
,actual_drop_off_longitude
,salik
,discount
,actual_interemirate_fee
,actual_total_cancellation_fee
,actual_parking_fee
,commission
,tips
,total_fare
,actual_peak_multiplier
,adjustmentamount
,actual_booking_extra_fee
,"cash collected"  
,actual_payment_instrument
,dispatchfeeamount
,netfare

,rta_revised_fare_adjustment
,vat_on_comission

, CASE 
    WHEN car_category = 'LIMO'  AND TRIM(UPPER(payment_method)) NOT LIKE '%CASH%' 
        THEN (COALESCE(Net_Earning_1, 0) - COALESCE(dispatchFeeAmount, 0) + COALESCE(actual_total_cancellation_fee, 0))
    WHEN car_category = 'LIMO'  AND TRIM(UPPER(payment_method))     LIKE '%CASH%' 
        THEN (COALESCE(Net_Earning_1, 0) - COALESCE(dispatchFeeAmount, 0) + COALESCE(discount, 0) + COALESCE(actual_total_cancellation_fee, 0))
    WHEN car_category = 'TAXI'  AND TRIM(UPPER(payment_method)) NOT LIKE '%CASH%' 
        THEN (COALESCE(Net_Earning_1, 0))
    WHEN car_category = 'TAXI'  AND TRIM(UPPER(payment_method))     LIKE '%CASH%' 
        THEN (COALESCE(Net_Earning_1, 0) + COALESCE(discount, 0))
  END
+ COALESCE(tips, 0)           AS Net_Earning
 

, case when car_category = 'LIMO'  and TRIM(UPPER(payment_method)) NOT LIKE '%CASH%' then 
(COALESCE(rta_revised_fare_adjustment,0) + COALESCE(Net_Earning_1,0)) - coalesce (dispatchFeeAmount,0)  + coalesce(actual_total_cancellation_fee,0)

 when car_category = 'LIMO'  and TRIM(UPPER(payment_method))  LIKE '%CASH%' then 
(COALESCE(rta_revised_fare_adjustment,0) + COALESCE(Net_Earning_1,0)) - coalesce (dispatchFeeAmount,0) +coalesce(discount,0) + coalesce(actual_total_cancellation_fee,0)


when car_category = 'TAXI'  and TRIM(UPPER(payment_method))  NOT LIKE '%CASH%' then 
 (COALESCE(rta_revised_fare_adjustment,0) + COALESCE(Net_Earning_1,0))

 when car_category = 'TAXI'  and TRIM(UPPER(payment_method))   LIKE '%CASH%' then
 
 (COALESCE(rta_revised_fare_adjustment,0) + COALESCE(Net_Earning_1,0)) + +coalesce(discount,0) 
 end 
 + COALESCE(tips, 0)
 
as final_net_payable_to_supplier
,CURRENT_TIMESTAMP as query_run_timestamp

from
(
select 
*
, netFare - (case when trim(upper(payment_method)) like '%CASH%' then COALESCE(total_fare,0) + COALESCE(adjustmentamount ,0) else 0 end) as Net_Earning_1-- money to be transferred  to 

,case when trip_status in ('PAYMENT_RECEIVED','JOURNEY_COMPLETED') and netFare > 0 then -1* (actual_booking_extra_fee) else 0 end as rta_revised_fare_adjustment 


,0.05*commission  as Vat_on_comission -- for taxi the commission includes this so we dont deduct for limo we deduct extra 5% from supplier as vat
from base_2 
)







 
