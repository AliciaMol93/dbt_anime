{{ config(
    materialized='table',
    tags=['marts','dimension'],
    max_recursion_depth=200000
) }}

WITH raw_stats AS (
    SELECT MIN(CAST(ingestion_ts AS DATE)) AS min_dt_stats
    FROM {{ ref('stg_anime__stats') }}
),

raw_titles AS (
    SELECT MIN(CAST(ingestion_ts AS DATE)) AS min_dt_titles
    FROM {{ ref('stg_anime__titles') }}
),

date_range AS (
    SELECT
        COALESCE(
            LEAST(min_dt_stats, min_dt_titles),
            '2020-01-01'::DATE
        ) AS min_dt,
        DATEADD(day, 365, CURRENT_DATE()) AS max_dt
    FROM raw_stats, raw_titles
),

recursive_dates AS (
    SELECT min_dt AS date, max_dt
    FROM date_range

    UNION ALL

    SELECT DATEADD(day, 1, date), max_dt
    FROM recursive_dates
    WHERE date < max_dt
),

final AS (
    SELECT
        date,
        TO_CHAR(date, 'YYYYMMDD')::NUMBER AS date_id,
        EXTRACT(YEAR FROM date) AS year,
        EXTRACT(MONTH FROM date) AS month,
        EXTRACT(DAY FROM date) AS day,
        CASE
            WHEN EXTRACT(MONTH FROM date) IN (12,1,2) THEN 'Winter'
            WHEN EXTRACT(MONTH FROM date) IN (3,4,5) THEN 'Spring'
            WHEN EXTRACT(MONTH FROM date) IN (6,7,8) THEN 'Summer'
            ELSE 'Fall'
        END AS season
    FROM recursive_dates
)

SELECT *
FROM final
ORDER BY date
