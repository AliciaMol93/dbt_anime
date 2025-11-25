{{ config(
    materialized="table",
    unique_key=["person_id", "anime_id", "role"]
) }}

WITH src AS (
    SELECT
        {{ surrogate_key(['PERSON_MAL_ID']) }} AS person_id,
        {{ surrogate_key(['ANIME_MAL_ID']) }} AS anime_id,
        position
    FROM {{ ref('stg_anime__person_anime_works') }}
    WHERE PERSON_MAL_ID IS NOT NULL
      AND ANIME_MAL_ID IS NOT NULL
),

cleaned AS (
    SELECT
        person_id,
        anime_id,

        -- Normalize role
        REGEXP_REPLACE(
            LOWER(TRIM(position)),
            '\\s*\\(.*\\)',  
            ''
        ) AS role
    FROM src
),

dedup AS (
    SELECT DISTINCT
        person_id,
        anime_id,
        role
    FROM cleaned
)

SELECT *
FROM dedup
