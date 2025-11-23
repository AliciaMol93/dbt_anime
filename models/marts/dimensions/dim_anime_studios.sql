{{ config(materialized="table") }}

select
    r.anime_id,
    s.studio_id,
    s.studio_name
from {{ ref('stg_anime__studio_rel_anime') }} r
join {{ ref('stg_anime__studios') }} s
    on r.studio_id = s.studio_id
