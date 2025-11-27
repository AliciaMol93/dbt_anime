{{ config(materialized="table") }}

WITH src AS (
    SELECT 
        anime_id, 
        title, 
        type, 
        year, 
        season
    FROM {{ ref("dim_anime_anime") }}
    WHERE lower(title) LIKE '%kimetsu%'
),

clean_versions AS (
    SELECT
        {{ surrogate_key(["anime_id"]) }} AS version_id,

        anime_id,
        title,
        type,
        season,
        year AS release_year,

        CASE
            WHEN title ILIKE '%Mugen Ressha-hen%' AND type = 'movie'
                THEN 'Mugen Train (Movie)'
            WHEN title ILIKE '%Mugen Ressha-hen%' AND type = 'tv'
                THEN 'Mugen Train (TV Arc)'
            WHEN title ILIKE '%Yuukaku%'
                THEN 'Entertainment District Arc'
            WHEN title ILIKE '%Katanakaji%'
                THEN 'Swordsmith Village Arc'
            WHEN title = 'Kimetsu no Yaiba' AND type = 'tv'
                THEN 'Season 1'
            ELSE initcap(title)
        END AS version_name,

        CASE
            WHEN title = 'Kimetsu no Yaiba' AND type = 'tv' 
                THEN 'S1'
            WHEN title ILIKE '%Yuukaku%' 
                THEN 'S2'
            WHEN title ILIKE '%Katanakaji%' 
                THEN 'S3'
            WHEN title ILIKE '%Mugen%' 
                THEN 'Movie'
            ELSE 'Other'
        END AS canonical_group,

        CASE 
            WHEN canonical_group IN ('S1', 'S2', 'S3', 'Movie') 
                THEN TRUE 
            ELSE FALSE 
        END AS is_canon,

        CASE 
            WHEN type IN ('tv', 'movie') THEN TRUE 
            ELSE FALSE 
        END AS is_main_line,

        'Kimetsu no Yaiba' AS universe

    FROM src
    WHERE type IN ('tv', 'movie')
)

SELECT *
FROM clean_versions
