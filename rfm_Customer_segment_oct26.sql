drop table prod_etl_temp.customer_rfm_segments;
create table prod_etl_temp.customer_rfm_segments as

WITH params AS 
(
 SELECT (CONVERT_TIMEZONE('UTC', 'Asia/Dubai', GETDATE())::date) AS as_of_date
),

active_customers AS (
    SELECT DISTINCT useruid AS customer_id
    FROM public.users
    WHERE status = 1
),

wallet_debit_trips AS (
    SELECT
        reference_id AS ref_journey_id,
        SUM(amount)  AS wallet_amount
    FROM zed_stripe.wallet_ledger
    WHERE UPPER(TRIM(type)) = 'DEBIT'
      AND transaction_description NOT IN ('EXPIRED', 'CASHBACK_CLAWBACK')
      AND reference_id IS NOT NULL
      AND wallet_id IS NULL
    GROUP BY 1
),

trips AS (
    SELECT
        jmst.customer_id                                                AS customer_id,
        jmst.journey_id,
        CONVERT_TIMEZONE('UTC', 'Asia/Dubai', jmst.pickup_time)         AS pickup_ts,
        CASE WHEN jmst.ref_promo_applied IS NOT NULL THEN 1 ELSE 0 END  AS is_promo,
        COALESCE(jmst.actual_total_fee, 0)                              AS fare,
        COALESCE(jmst.actual_discount_amount, 0)                        AS promo_amount,
        COALESCE(wd.wallet_amount, 0)                                   AS wallet_amount
    FROM prod_etl_data.tbl_journey_master jmst
    INNER JOIN active_customers ac
        ON ac.customer_id = jmst.customer_id
    LEFT JOIN wallet_debit_trips wd
        ON jmst.ref_journey_id = wd.ref_journey_id
    WHERE jmst.journey_status IN (9, 10)
     AND CONVERT_TIMEZONE('UTC', 'Asia/Dubai', jmst.pickup_time)::date <= (SELECT as_of_date FROM params)
),

ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY pickup_ts DESC) AS rn
    FROM trips
),

cust AS (
    SELECT
        t.customer_id,
        MAX(t.pickup_ts)::date                                             AS last_trip_date,
        DATEDIFF(day, MAX(t.pickup_ts)::date, p.as_of_date)                AS days_since_last,
        COUNT(DISTINCT t.journey_id)                                       AS lifetime_trips,
        SUM(CASE WHEN t.pickup_ts::date >= DATEADD(day, -90, p.as_of_date)
                 THEN 1 ELSE 0 END)                                        AS trips_90d,
        SUM(t.fare)                                                        AS gross_fare,
        SUM(t.promo_amount)                                                AS promo_burn,
        SUM(t.wallet_amount)                                               AS wallet_burn,
        SUM(t.fare - t.promo_amount - t.wallet_amount)                     AS net_rev,
        SUM(t.is_promo)                                                    AS lifetime_promo_trips,
        SUM(CASE WHEN r.rn <= 10 THEN 1 ELSE 0 END)                        AS last10_trips,
        SUM(CASE WHEN r.rn <= 10 THEN t.is_promo ELSE 0 END)               AS last10_promo_trips
    FROM ranked r
    JOIN trips t ON t.journey_id = r.journey_id AND t.customer_id = r.customer_id
    CROSS JOIN params p
    GROUP BY 1, p.as_of_date
),

metrics AS (
    SELECT
        *,
        ROUND(net_rev::float / lifetime_trips, 2)                              AS m_value,
        ROUND(100.0 * last10_promo_trips / NULLIF(last10_trips, 0), 2)         AS last10_promo_pct,
        ROUND(100.0 * (promo_burn + wallet_burn) / NULLIF(gross_fare, 0), 2)   AS burn_pct
    FROM cust
),

segmented AS (
    SELECT
        *,
        CASE
            WHEN days_since_last <= 30  AND trips_90d >= 12      THEN '01_Champion'
            WHEN days_since_last <= 30  AND trips_90d >= 5       THEN '02_Regular'
            WHEN days_since_last <= 30  AND trips_90d >= 2       THEN '03_Occasional'
            WHEN days_since_last <= 30  AND lifetime_trips = 1   THEN '04_New'
            WHEN days_since_last <= 30                           THEN '05_Returning'
            WHEN days_since_last <= 60  AND lifetime_trips >= 5  THEN '06_AtRisk_Frequent'
            WHEN days_since_last <= 60  AND lifetime_trips >= 2  THEN '07_AtRisk_Casual'
            WHEN days_since_last <= 60                           THEN '08_AtRisk_One_Trip'
            WHEN days_since_last <= 90  AND lifetime_trips >= 5  THEN '09_Dormant_Frequent'
            WHEN days_since_last <= 90  AND lifetime_trips >= 2  THEN '10_Dormant_Casual'
            WHEN days_since_last <= 90                           THEN '11_Dormant_One_Trip'
            WHEN days_since_last <= 180                          THEN '12_Dark'
            WHEN days_since_last <= 365                          THEN '13_Lost'
            ELSE                                                      '14_Archive'
        END AS base_segment
    FROM metrics
)


SELECT
    customer_id,
    last_trip_date,
    days_since_last,
    lifetime_trips,
    trips_90d,
    lifetime_promo_trips,
    gross_fare,
    promo_burn,
    wallet_burn,
    net_rev,
    m_value,
    last10_promo_pct,
    burn_pct,
    base_segment                             AS rfm_segment,

    CASE WHEN days_since_last <= 30  THEN 'R1_Active'
         WHEN days_since_last <= 60  THEN 'R2_Inactive'
         WHEN days_since_last <= 90  THEN 'R3_Dormant'
         WHEN days_since_last <= 180 THEN 'R4_Lapsed'
         WHEN days_since_last <= 365 THEN 'R5_Lost'
         ELSE                             'R6_Archive' END              AS r_tier,

    CASE WHEN lifetime_trips = 1   THEN 'F1_Single'
         WHEN lifetime_trips <= 4  THEN 'F2_Casual'
         WHEN lifetime_trips <= 10 THEN 'F3_Regular'
         WHEN lifetime_trips <= 24 THEN 'F4_Power'
         ELSE                           'F5_Super' END                  AS f_tier_lifetime,

    CASE WHEN lifetime_trips < 5 THEN 'M_NA'
         WHEN m_value > 45       THEN 'M_High'
         WHEN m_value >= 31      THEN 'M_Mid'
         ELSE                         'M_Low' END                       AS m_tier,

    CASE
        WHEN lifetime_trips <= 4 AND lifetime_promo_trips = 0              THEN 'No_Promo'
        WHEN lifetime_trips <= 4 AND lifetime_promo_trips = lifetime_trips THEN 'All_Promo'
        WHEN lifetime_trips <= 4                                           THEN 'Some_Promo'
        WHEN last10_promo_pct = 0                                          THEN 'Organic'
        WHEN last10_promo_pct < 50                                         THEN 'Light'
        WHEN last10_promo_pct < 80                                         THEN 'Heavy'
        ELSE                                                                    'Dependent'
    END AS promo_tag,

    CASE WHEN burn_pct = 0    THEN 'B0_0pct'
         WHEN burn_pct <= 10  THEN 'B1_0-10pct'
         WHEN burn_pct <= 25  THEN 'B2_10-25pct'
         ELSE                      'B3_25pct_plus' END                  AS burn_band,

    (SELECT as_of_date FROM params)          AS snapshot_date
FROM segmented
