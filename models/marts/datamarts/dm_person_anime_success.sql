{{ config(
    materialized = "table"
) }}

-- Personas con astrología
WITH person_dim AS (
    SELECT
        person_id,
        zodiac_sign,
        generation
    FROM {{ ref('dim_anime_person_astrology') }}
),

-- Roles de persona en anime
role_links AS (
    SELECT
        person_id,
        anime_id,
        role
    FROM {{ ref('dim_anime_person_works')}}
),

-- Última fila del fact diario para cada anime
latest_anime AS (
    SELECT *
    FROM (
        SELECT
            anime_id,
            avg_score,
            success_index,
            total_engagement,
            stat_date,
            ROW_NUMBER() OVER (PARTITION BY anime_id ORDER BY stat_date DESC) AS rn
        FROM {{ ref('fact_anime_performance_daily')}}
    )
    WHERE rn = 1
)

SELECT
    r.person_id,
    r.anime_id,
    r.role,                   

    p.zodiac_sign,
    p.generation,

    a.avg_score,
    a.success_index,
    a.total_engagement,
    a.stat_date AS latest_stat_date

FROM role_links r
LEFT JOIN person_dim p USING (person_id)
LEFT JOIN latest_anime a USING (anime_id)

WHERE p.zodiac_sign IS NOT NULL
