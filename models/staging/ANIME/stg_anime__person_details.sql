{{ config(
    materialized='table'
) }}

WITH src_person_details AS (
    SELECT
        PERSON_MAL_ID AS person_mal_id,
        NULLIF(TRIM(NAME)), '') AS name,
        NULLIF(TRIM(GIVEN_NAME)), '') AS given_name,
        NULLIF(TRIM(FAMILY_NAME)), '') AS family_name,
        CAST(BIRTHDAY AS DATE) AS birthday,
        CAST(FAVORITES AS INT) AS favorites
    FROM {{ source('anime_source', 'PERSON_DETAILS') }}
    WHERE PERSON_MAL_ID IS NOT NULL
)

SELECT
    {{ surrogate_key(['person_mal_id']) }} AS person_id,
    name,
    given_name,
    family_name,
    birthday,
    favorites,
    CURRENT_TIMESTAMP() AS last_updated_at
FROM src_person_details
