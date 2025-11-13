{{ config(
    materialized='table',
    unique_key=['person_id']
) }}

SELECT
    PERSON_MAL_ID AS person_mal_id,
    {{ dbt_utils.generate_surrogate_key(['person_mal_id']) }} as person_id,
    LOWER(TRIM(name)) as name,
    LOWER(TRIM(given_name)) as given_name,
    LOWER(TRIM(family_name)) as family_name,
    CAST(birthday AS DATE) as birthday,
    favorites   
    
FROM {{ source('anime_source', 'PERSON_DETAILS') }}

