{{ config(materialized="table") }}

select 
    anime_id, 
    producer_id, 
    {{ surrogate_key(['anime_id', 'producer_id']) }} as anime_producer_key
from {{ ref("base_anime__producers") }}
where producer_name is not null
