{{ config(materialized="table") }}

select
    rel.anime_id,
    prod.producer_id,
    prod.producer_name
from {{ ref('stg_anime__producers_rel_anime') }} rel
join {{ ref('stg_anime__producers') }} prod
    on rel.producer_id = prod.producer_id
