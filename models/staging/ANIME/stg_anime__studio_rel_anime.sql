{{ config(
    materialized='table'
) }}

select
    anime_id,
    studio_id,
    {{ surrogate_key(['anime_id', 'studio_id']) }} as anime_studio_key
from {{ ref("base_anime__studios") }}
where studio_name is not null