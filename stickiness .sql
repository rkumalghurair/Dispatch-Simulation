-- Stickiness ladder: P(trip N+1 within 30 days of trip N)
-- Cohort-anchored on first-ever completed trip, censoring-safe.

WITH completed AS (
    SELECT
        jm.ref_customer_id AS customer_id,
		ref_promo_applied ,
        CONVERT_TIMEZONE('UTC','Asia/Dubai', jm.journey_created_at) AS trip_ts
    FROM prod_etl_data.tbl_journey_master jm
	inner join (select _id from public.users where status =1)u on jm.ref_customer_id=u._id
    WHERE jm.journey_status IN (9,10)
),

first_trip AS (
    SELECT customer_id, MIN(trip_ts) AS first_trip_ts
    FROM completed
    GROUP BY 1
),

-- acquisition window: mature enough that trip 1 has a full 30-day look-ahead
cohort AS (
    SELECT customer_id, first_trip_ts
    FROM first_trip
    WHERE first_trip_ts >= '2026-05-01'
      AND first_trip_ts <  '2026-07-01'
),

seq AS (
    SELECT
        c.customer_id,
        c.trip_ts,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY c.trip_ts) AS trip_seq,
        LEAD(c.trip_ts)  OVER (PARTITION BY c.customer_id ORDER BY c.trip_ts) AS next_trip_ts
    FROM completed c
    JOIN cohort k ON k.customer_id = c.customer_id
)
,ladder as(
select 
trip_seq
, customer_id
, CASE WHEN next_trip_ts IS NOT NULL AND DATEDIFF(day, trip_ts, next_trip_ts) <= 30 THEN 1 ELSE 0 END AS continued 
, CASE WHEN DATEDIFF(day, trip_ts, CONVERT_TIMEZONE('UTC','Asia/Dubai', GETDATE())) >= 30 THEN 1 ELSE 0 END AS fully_observed
from seq
)


,

rates AS (
    SELECT
        trip_seq                                        AS trip_number,
        COUNT(distinct customer_id)                                        AS users_at_trip_n,
        SUM(continued)                                  AS came_back_for_next,
        1.0 * SUM(continued) / NULLIF(COUNT(distinct customer_id),0)       AS continue_rate
    FROM ladder
    WHERE trip_seq <= 15
      -- keep users who returned even if their window isn't complete;
      -- drop only the "no return yet, window still open" cases
      AND (fully_observed = 1 OR continued = 1)
    GROUP BY 1
	order by 1
)
select * from rates
