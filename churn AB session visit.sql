WITH test_users AS (
    SELECT DISTINCT customer_id, churn_risk_segment, experiment_group, DATE(ingested_at) AS target_dt
    FROM prod_etl_temp.new_user_churn_scores
    WHERE DATE(ingested_at) >= '2026-07-29'
    AND experiment_group = 'Test'
)

,all_completed AS (
    SELECT j.ref_customer_id, j.customer_id,
        (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
        DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS journey_dt
    FROM prod_etl_data.tbl_journey_master j
    WHERE j.journey_status IN (9,10) AND j.journey_type = 1
),
trip_ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ref_customer_id ORDER BY journey_ts ASC) AS trip_rank
    FROM all_completed
),
first_trips AS (
    SELECT ref_customer_id, customer_id, journey_ts AS first_trip_ts
    FROM trip_ranked
    WHERE trip_rank = 1
),

test_with_trips AS (
    SELECT tu.customer_id, tu.churn_risk_segment, tu.target_dt,
           ft.first_trip_ts, ft.ref_customer_id
    FROM test_users tu
    LEFT JOIN first_trips ft ON ft.customer_id = tu.customer_id
)
-- select count(*) from test_with_trips
,
app_events as
(
    select 
    user_id,event_time, session_id 
    from amplitude_customer_app.events 
    where date (event_time)>='2026-07-25'
)

,app_sessions AS 
(
SELECT DISTINCT twt.customer_id, twt.churn_risk_segment, twt.target_dt
FROM test_with_trips twt
 INNER JOIN app_events e
        ON (e.user_id = twt.customer_id )
        AND (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' >= twt.target_dt::timestamp       
    WHERE 
    e.session_id <> -1
    AND e.session_id IS NOT NULL
    AND e.user_id IS NOT NULL
)


SELECT
    tu.churn_risk_segment,
    COUNT(DISTINCT tu.customer_id)          AS total_test_users,
    COUNT(DISTINCT ap.customer_id)          AS came_back_to_app,
    COUNT(DISTINCT tu.customer_id) 
        - COUNT(DISTINCT ap.customer_id)    AS did_not_come_back,
    ROUND(COUNT(DISTINCT ap.customer_id) * 100.0 
        / NULLIF(COUNT(DISTINCT tu.customer_id), 0), 1) AS pct_came_back
FROM test_users tu
LEFT JOIN app_sessions ap ON ap.customer_id = tu.customer_id
GROUP BY tu.churn_risk_segment
ORDER BY tu.churn_risk_segment


