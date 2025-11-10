with cust_details as(
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
 -- --187/19274
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
    and TO_TIMESTAMP(initiated_at, 'MM/DD/YYYY HH24:MI')  <= '2025-10-01'
)
, min_initiated_Date as 
(
select 
ref_Customer_id
,customer_id
, min(initiated_date)as initiated_date
from  
joined_details group by 1,2
)
-- select count(distinct ref_customer_id) from min_initiated_Date;--6986

-- select * from min_initiated_Date where ref_Customer_id ='675c791e934db4608e384105';
-- select * from min_initiated_Date where customer_id ='CUS_ZSSQH62513';
--6986
-- select min(initiated_date),max(initiated_date) from joined_details ; -- Data from July to Sept
 -- 2025-07-01 and 2025-11-06
,jrny_details as
(
select 
(journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as local_created_time
,journey_id
,ref_customer_id
,customer_id
,ref_promo_applied
,journey_status
,case when journey_status in (13) then  journey_id end as customer_cancelled
,case when journey_status in (9,10) then journey_id end as completed_jrny
from 
prod_etl_data.tbl_journey_master  
where 
date ((journey_created_at::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < current_Date
)
-- select count(distinct journey_id), count(case when journey_Status in (9,10) then journey_id end ) 
-- from jrny_details where ref_customer_id ='6783adb10f438601d5d2a742' and  date(local_created_time) >='2025-07-20';
--  and ref_promo_applied is not null;
-- 76	33	3

,pre_ticket_summary AS (
    SELECT 
        joined.ref_customer_id,
        joined.initiated_date,
        COUNT(DISTINCT CASE WHEN jrny.local_created_time < joined.initiated_date AND jrny.journey_status IN (9,10) THEN jrny.journey_id END) AS completed_before,
        COUNT(DISTINCT CASE WHEN jrny.local_created_time < joined.initiated_date THEN jrny.journey_id END) AS total_before
    FROM 
    min_initiated_Date  as joined
    LEFT JOIN jrny_details jrny
        ON joined.ref_customer_id = jrny.ref_customer_id
    GROUP BY 1,2
)
-- select * from pre_ticket_summary where ref_customer_id='6783adb10f438601d5d2a742';

,customer_segment AS (
    SELECT 
        ref_customer_id,
        initiated_date,
        CASE 
            WHEN completed_before = 0 THEN 'No Completed Trip Before Ticket'
            WHEN completed_before = 1 THEN '1 Completed Trip Before Ticket'
            WHEN completed_before > 1 and  completed_before < 5  THEN '2_5_completed_trips'
             WHEN completed_before >= 5   THEN 'More than 5 completed trips'
        END AS pre_ticket_category
    FROM pre_ticket_summary
)
--  select *  from customer_segment;

,post_ticket_behavior AS (
    SELECT 
        t.ref_customer_id,
        t.initiated_date,
        COUNT(DISTINCT CASE WHEN j.local_created_time > t.initiated_date THEN j.journey_id END) AS trips_after_ticket,
        COUNT(DISTINCT CASE WHEN j.local_created_time > t.initiated_date AND j.journey_status IN (9,10)THEN j.journey_id END) AS completed_after_ticket,
        COUNT(DISTINCT CASE WHEN j.local_created_time > t.initiated_date AND j.journey_status IN (9,10) and ref_promo_applied is not null THEN j.journey_id END) AS completed_after_ticket_with_promo
    FROM 
    min_initiated_Date t
    LEFT JOIN jrny_details j
        ON t.ref_customer_id = j.ref_customer_id
    GROUP BY 1,2
)
--  select * from post_ticket_behavior where ref_customer_id='6783adb10f438601d5d2a742';
SELECT 
    s.pre_ticket_category,
    COUNT(DISTINCT s.ref_customer_id) AS total_customers,
    COUNT(DISTINCT CASE WHEN p.completed_after_ticket > 0 THEN s.ref_customer_id END) AS returned_customers,
    ROUND(
        COUNT(DISTINCT CASE WHEN p.completed_after_ticket > 0 THEN s.ref_customer_id END)::DECIMAL /
        NULLIF(COUNT(DISTINCT s.ref_customer_id), 0), 3
    ) AS return_rate_post_ticket,
    SUM(p.completed_after_ticket_with_promo)::NUMERIC / NULLIF(SUM(p.completed_after_ticket), 0) AS perc_trips_with_promo,
    COUNT(DISTINCT CASE WHEN p.completed_after_ticket_with_promo > 0 THEN s.ref_customer_id END) as returned_customer_with_promo,


     COUNT(DISTINCT CASE WHEN p.completed_after_ticket_with_promo > 0 and p.completed_after_ticket > 0  THEN s.ref_customer_id END)::DECIMAL /COUNT(DISTINCT CASE WHEN p.completed_after_ticket > 0 THEN s.ref_customer_id END) as returned_customer_with_promo


FROM 
customer_segment s
LEFT JOIN post_ticket_behavior p
    ON s.ref_customer_id = p.ref_customer_id
GROUP BY 1
ORDER BY 1;
