{{ config(materialized="table") }}

select
    r.anime_id,
    p.producer_id,
    p.producer_name
from {{ ref('stg_anime__producers_rel_anime') }} r
join {{ ref('stg_anime__producers') }} p
    on r.producer_id = p.producer_id
