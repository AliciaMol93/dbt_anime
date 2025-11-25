{{ config(
    materialized="table",
    tags=['marts','dimension']
) }}

with theme_rel as (
    select
        anime_id,
        theme_id
    from {{ ref('stg_anime__theme_rel_anime') }}
),

theme_base as (
    select
        theme_id,
        theme_name
    from {{ ref('stg_anime__themes') }}
)

select
    r.anime_id,
    t.theme_id,
    t.theme_name
from theme_rel r
join theme_base t
    on r.theme_id = t.theme_id
where r.anime_id is not null
