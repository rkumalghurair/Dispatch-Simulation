WITH
all_completed AS (
    SELECT j.ref_customer_id, j.customer_id, j.journey_id,
        (j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS journey_ts,
        DATE((j.journey_created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai') AS journey_dt
    FROM prod_etl_data.tbl_journey_master j
    WHERE j.journey_status IN (9,10) AND j.journey_type = 1
),
trip_ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ref_customer_id ORDER BY journey_ts ASC) AS trip_rank
    FROM all_completed
)
-- select * from trip_ranked where customer_id='CUS_DVZZ7019';
-- 65dc79fab17ddd0b3480a2c1	1781158766412	2026-06-11 10:19:54.403	93	user_loggedin -> otp_entered -> page_view:LocationPermission -> location_permission_allowed -> page_view:NotificationPermission -> notification_permission_allowed -> page_view:Home -> referred_code_action -> sheet_view -> journeyintent_started -> page_view:LocationSelection -> location_search -> drop_location_set -> continue_cta_tapped -> drop_location_set -> page_view:VehicleCategories -> payment_sheet_opened -> promotion_sheet_opened -> promo_cta_tapped -> promotion_code_added -> vehicle_category_set -> book_journey_clicked -> page_view:JourneyDetailsScreen	4	

,
first_trips AS (
    SELECT ref_customer_id, customer_id, journey_dt AS first_trip_dt,
    journey_ts   AS first_trip_ts   
    FROM trip_ranked
    WHERE trip_rank = 1 AND journey_dt >= '2026-05-01' AND journey_dt < '2026-07-01'
),
label AS (
    SELECT ft.ref_customer_id, ft.customer_id, ft.first_trip_dt, ft.first_trip_ts,
        CASE WHEN MIN(tr.journey_dt) IS NOT NULL THEN 0 ELSE 1 END AS churned_21d
    FROM first_trips ft
    LEFT JOIN trip_ranked tr
           ON tr.ref_customer_id = ft.ref_customer_id
          AND tr.trip_rank >= 2
          AND tr.journey_dt >= ft.first_trip_dt
          AND tr.journey_dt <= DATEADD(day, 21, ft.first_trip_dt)
    GROUP BY ft.ref_customer_id, ft.customer_id, ft.first_trip_dt,first_trip_ts
),
churned_users AS (
    SELECT ref_customer_id, customer_id, first_trip_dt, first_trip_ts FROM label WHERE churned_21d = 1
)
-- select count(*), count(distinct customer_id) from churned_users --23,269

-- ── All app events for churned users, within their 21-day observation window ──
,amp_events AS (
    SELECT cu.ref_customer_id, e.session_id,
           (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS event_ts,
           LOWER(TRIM(e.event_type)) AS event_type,
           json_extract_path_text(e.event_properties, 'screen') AS screen
    FROM amplitude_customer_app.events e
    INNER JOIN churned_users cu ON cu.customer_id = e.user_id
      AND (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
               > cu.first_trip_ts                                      -- CHANGED: strict > on the real ts, not BETWEEN first_trip_dt
           AND (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
               <= DATEADD(day, 21, cu.first_trip_ts)  
    WHERE e.session_id <> -1 AND e.session_id IS NOT NULL AND e.user_id IS NOT NULL

    UNION ALL

    SELECT cu.ref_customer_id, e.session_id,
           (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai' AS event_ts,
           LOWER(TRIM(e.event_type)) AS event_type,
           json_extract_path_text(e.event_properties, 'screen') AS screen
    FROM amplitude_customer_app.events e
    INNER JOIN churned_users cu ON cu.ref_customer_id = e.user_id
         AND (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
               > cu.first_trip_ts                                      -- CHANGED: strict > on the real ts, not BETWEEN first_trip_dt
           AND (e.event_time::timestamp AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Dubai'
               <= DATEADD(day, 21, cu.first_trip_ts)  
    WHERE e.session_id <> -1 AND e.session_id IS NOT NULL AND e.user_id IS NOT NULL
      AND e.user_id NOT LIKE 'CUS_%'
)
-- select * from amp_events where ref_customer_id='65dc79fab17ddd0b3480a2c1' order by event_ts desc;
-- select count(*), count(distinct concat (ref_customer_id,session_id,event_type)) from amp_events;

-- ── Build the raw path token per event, excluding telemetry/polling noise ────
,raw_stage AS (
    SELECT ref_customer_id, session_id, event_ts,
        CASE WHEN event_type = 'page_view' AND screen IS NOT NULL
             THEN event_type || ':' || screen
             ELSE event_type
        END AS token
    FROM amp_events
    WHERE event_type NOT IN (
        '[amplitude] application opened',
        '[amplitude] application backgrounded',
        '[amplitude] application updated',
        'session_start', 'session_end',
        'eta_refresh'
    )
)
-- select * from raw_stage where ref_customer_id ='68e68ee2743766813d409af4'  order by event_ts;


-- ── Collapse consecutive duplicate tokens (filter noise FIRST, then LAG) ─────
,raw_stage_flagged AS (
    SELECT *,
        CASE WHEN token = LAG(token) OVER (  PARTITION BY ref_customer_id, session_id ORDER BY event_ts  ) THEN 0 ELSE 1 END AS is_new_token
    FROM raw_stage
)
-- select * from raw_stage_flagged where ref_customer_id ='68e68ee2743766813d409af4'  order by event_ts;
,
raw_stage_clean AS (
    SELECT ref_customer_id, session_id, event_ts, token
    FROM raw_stage_flagged
    WHERE is_new_token = 1
)


-- select * from raw_stage_clean where ref_customer_id ='68e68ee2743766813d409af4';

-- ── Session-level: path string + duration ────────────────────────────────────
,sessions AS (
    SELECT ref_customer_id, session_id,
           MIN(event_ts) AS session_start_ts,
           DATEDIFF(second, MIN(event_ts), MAX(event_ts)) AS session_duration_sec,
           LISTAGG(token, ' -> ') WITHIN GROUP (ORDER BY event_ts) AS raw_path
    FROM raw_stage_clean
    GROUP BY ref_customer_id, session_id
)
-- select * from sessions where ref_customer_id='68e68ee2743766813d409af4';


-- ── Total unique post-first-trip sessions per churned user ───────────────────
,user_session_counts AS (
    SELECT ref_customer_id, COUNT(DISTINCT session_id) AS total_sessions_post_trip
    FROM sessions
    GROUP BY ref_customer_id
),

-- ── Each user's LAST session before going silent ─────────────────────────────
last_session AS (
    SELECT s.*, ROW_NUMBER() OVER (PARTITION BY s.ref_customer_id ORDER BY s.session_start_ts DESC) AS rn
    FROM sessions s
)
-- select * from last_session where ref_customer_id='65dc79fab17ddd0b3480a2c1' order by event_ts desc;

-- select * from last_session where raw_path like '%page_view:LocationSelection%' order by ref_customer_id, session_start_ts;
--  select * from last_session where ref_customer_id='68e68ee2743766813d409af4';

SELECT
    raw_path AS last_session_path,
    COUNT(*) AS n_users,
    ROUND(median(session_duration_sec), 0) AS avg_session_duration_sec,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_churned_users_with_a_session
FROM last_session
WHERE rn = 1
GROUP BY raw_path
ORDER BY n_users DESC
