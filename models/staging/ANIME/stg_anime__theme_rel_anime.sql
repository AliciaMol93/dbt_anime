{{ config(
    materialized='table'
) }}


select
    anime_id,
    theme_id,
    {{ surrogate_key(['anime_id', 'theme_id']) }} as anime_theme_key
from {{ ref('base_anime__themes') }} 
where theme_name is not null