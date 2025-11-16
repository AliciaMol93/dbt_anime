{{ config(
    materialized='table',
    unique_key=['person_id']
) }}

WITH src_about AS (
    SELECT
        PERSON_MAL_ID AS person_mal_id,
        REGEXP_REPLACE(TRIM(ABOUT), '\\n+', ' ') AS narrative_about_text
    FROM {{ source('anime_source', 'PERSON_DETAILS') }}
    WHERE ABOUT IS NOT NULL AND PERSON_MAL_ID IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['person_mal_id']) }} AS person_id,
    person_mal_id,
    narrative_about_text,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM src_about
