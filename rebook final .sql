WITH RECURSIVE chain (
    journey_id,
    ref_journey_id,
    ref_customer_id,
    ref_parent_journey_id,
    journey_status,
    journey_created_at,
    level,
    root_ref_journey_id       -- this is the chain ID carried across all levels
) AS (
    -- Anchor: parent journeys (start of each chain)
    SELECT 
        journey_id,
        ref_journey_id,
        ref_customer_id,
        ref_parent_journey_id,
        journey_status,
        (journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' as journey_created_at,
        0 AS level,
        ref_journey_id AS root_ref_journey_id   -- root of the chain
    FROM prod_etl_data.tbl_journey_master
    WHERE ref_parent_journey_id IS NULL
      AND DATE((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') >= '2025-10-19'
      AND DATE((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') < current_Date
    --   '2025-11-03'

    UNION ALL

    -- Recursive: attach rebooks to the existing chain
    SELECT
        c.journey_id,
        c.ref_journey_id,
        c.ref_customer_id,
        c.ref_parent_journey_id,
        c.journey_status,
        (c.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' journey_created_at,

        p.level + 1,
        p.root_ref_journey_id                  -- keep the same chain id from parent
    FROM prod_etl_data.tbl_journey_master c
    JOIN chain p 
      ON c.ref_parent_journey_id = p.ref_journey_id
),

ordered AS (
    SELECT 
        root_ref_journey_id AS chain_id,        -- this is base only journey
        journey_id,
        journey_status,
        level,
        journey_created_at,
        CASE WHEN journey_status = 13 THEN 1 ELSE 0 END AS is_customer_cancel
    FROM chain
)
-- select count(*), count(distinct chain_id), count(Distinct journey_id) from ordered;
-- the below sub query helps identify how many trips exceeeded the customer canceled limit etc
,final_chain_1 AS (
  select * from(
    SELECT
        root_ref_journey_id,
        MAX(level) AS max_level,
        sum(CASE WHEN journey_status IN (9,10) THEN 1 ELSE 0 END) AS chain_completed,
        sum(CASE WHEN journey_status = 13 THEN 1 ELSE 0 END) AS has_customer_cancel,
        sum(CASE WHEN journey_status = 12 THEN 1 ELSE 0 END) AS has_driver_cancel
    FROM chain
    GROUP BY 1
  ) 
  where has_customer_cancel >3
)
-- select * from final_chain_1 where root_ref_journey_id ='68fe84e4c8db11afe5564689';

-- now the mian query again 
, final_chain AS (
  SELECT
    root_ref_journey_id,
    ref_customer_id,
    MIN(journey_created_at) AS first_chain_time,
    MAX(journey_created_at) AS last_chain_time,
    MAX(level)              AS max_level,
    MAX(CASE WHEN journey_status IN (9,10) THEN 1 ELSE 0 END) AS chain_completed_flag,
    COUNT(DISTINCT journey_id) AS chain_journey_count
  FROM chain
  where root_ref_journey_id in (select distinct root_ref_journey_id from final_chain_1 )
  GROUP BY root_ref_journey_id, ref_customer_id
)

-- select * from final_chain_2 where root_ref_journey_id='68fe84e4c8db11afe5564689';


, next_parent_after_chain AS 
(
select
 *, DATEDIFF(minute, last_chain_time, next_parent_time) time_gap
  
, DATEDIFF(minute, last_chain_time, next_parent_time) time_gap_2
from
(
  SELECT
    f.ref_customer_id,
    f.root_ref_journey_id,
    f.first_chain_time,
    f.last_chain_time,
    f.max_level,
    f.chain_completed_flag,

    MIN(j2.journey_created_at) AS next_parent_time,
    MAX(CASE WHEN j2.journey_status IN (9,10) THEN 1 ELSE 0 END) AS next_parent_completed
  FROM final_chain f
  LEFT JOIN 
  (select 
  ref_parent_journey_id
  , ref_Customer_id
  , journey_id
  , journey_status
  ,(journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'  as journey_created_at 
  from 
  prod_etl_data.tbl_journey_master 
  where 
  date((journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' )>='2025-10-19'
  and ref_parent_journey_id is null

  )j2
    ON j2.ref_customer_id = f.ref_customer_id
   AND j2.ref_parent_journey_id IS NULL         -- new parent journey
   AND j2.journey_created_at > f.last_chain_time
   AND j2.journey_created_at <= last_chain_time + interval '15 minutes'

  GROUP BY 1,2,3,4,5,6
    
)
)


-- select * from next_parent_after_chain 
-- where next_parent_completed =1; 

-- WEEKLY AGGREGATED METRICS
SELECT
    DATE_TRUNC('week', first_chain_time)::date AS booking_week,

    COUNT(distinct root_ref_journey_id) AS total_chains,

    -- How many chains completed
    SUM(chain_completed_flag) AS completed_chains,
    ROUND(SUM(chain_completed_flag)::float / NULLIF(COUNT(*),0), 3) AS chain_completion_rate,
 

    -- Returned within 15 minutes
    SUM(CASE WHEN next_parent_time IS NOT NULL  AND DATEDIFF(minute, last_chain_time, next_parent_time) <= 15 THEN 1 ELSE 0 END) AS return_15min_count,

    ROUND(SUM(CASE WHEN next_parent_time IS NOT NULL AND DATEDIFF(minute, last_chain_time, next_parent_time) <= 15
             THEN 1 ELSE 0 END)::float 
          / NULLIF(COUNT(root_ref_journey_id),0), 3) AS return_15min_rate,

    -- Returned within 30 minutes
    SUM(CASE WHEN next_parent_time IS NOT NULL 
              AND DATEDIFF(minute, last_chain_time, next_parent_time) <= 30
             THEN 1 ELSE 0 END) AS return_30min_count,

    ROUND(SUM(CASE WHEN next_parent_time IS NOT NULL 
              AND DATEDIFF(minute, last_chain_time, next_parent_time) <= 30
             THEN 1 ELSE 0 END)::float 
          / NULLIF(COUNT(root_ref_journey_id),0), 3) AS return_30min_rate,

    -- How many returned *and completed*

ROUND(SUM(CASE WHEN next_parent_time IS NOT NULL AND DATEDIFF(minute, last_chain_time, next_parent_time) <= 30
             THEN 1 ELSE 0 END)::float 
          / NULLIF(COUNT(root_ref_journey_id),0), 3) AS return_30min_rate,



    SUM(CASE WHEN next_parent_completed=1  and DATEDIFF(minute, last_chain_time, next_parent_time) <= 15 THEN 1 ELSE 0 END) AS returned_and_completed_count,
    ROUND(SUM(CASE WHEN next_parent_completed = 1  and DATEDIFF(minute, last_chain_time, next_parent_time) <= 15THEN 1 ELSE 0 END)::float
          / NULLIF(COUNT(root_ref_journey_id),0), 3) AS returned_and_completed_rate

FROM next_parent_after_chain
GROUP BY 1
ORDER BY 1;
