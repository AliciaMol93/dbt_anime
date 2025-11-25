{{ config(
    materialized='table',
    unique_key=['person_id', 'anime_id', 'role']
) }}

WITH src AS (
    SELECT
        person_id,
        anime_id,
        lower(trim(position)) AS raw_role
    FROM {{ ref('stg_anime__person_anime_works') }}
    WHERE person_id IS NOT NULL 
      AND anime_id IS NOT NULL
),

clean_base AS (
    SELECT
        person_id,
        anime_id,
        REGEXP_REPLACE(raw_role, '\\s*\\(.*\\)', '') AS role_no_paren
    FROM src
),

normalized AS (
    SELECT
        person_id,
        anime_id,

        CASE
            WHEN role_no_paren ILIKE '%director%' THEN 'director'
            WHEN role_no_paren ILIKE '%storyboard%' THEN 'storyboard'
            WHEN role_no_paren ILIKE '%key animation%' THEN 'key animation'
            WHEN role_no_paren ILIKE '%animation director%' THEN 'animation director'
            WHEN role_no_paren ILIKE '%original creator%' THEN 'original creator'
            WHEN role_no_paren ILIKE '%theme song%' THEN 'theme song'
            WHEN role_no_paren ILIKE '%composition%' THEN 'composer'
            WHEN role_no_paren ILIKE '%lyrics%' THEN 'composer'
            WHEN role_no_paren ILIKE '%inserted song%' THEN 'music'
            WHEN role_no_paren ILIKE '%sound%' THEN 'sound'
            WHEN role_no_paren ILIKE '%mechanical%' THEN 'design'
            WHEN role_no_paren ILIKE '%character design%' THEN 'design'
            WHEN role_no_paren ILIKE '%layout%' THEN 'layout'
            WHEN role_no_paren ILIKE '%setting%' THEN 'setting'
            WHEN role_no_paren ILIKE '%background%' THEN 'background art'
            WHEN role_no_paren ILIKE '%producer%' THEN 'producer'
            ELSE role_no_paren
        END AS role
    FROM clean_base
),

dedup AS (
    SELECT DISTINCT person_id, anime_id, role
    FROM normalized
)

SELECT *
FROM dedup
