{{ config(
    materialized='table'
) }}

    select distinct
        theme_id,
        theme_name
    from {{ ref('base_anime__themes') }} 
    where theme_name is not null

