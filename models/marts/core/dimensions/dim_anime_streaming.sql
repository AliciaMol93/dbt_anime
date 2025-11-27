
{{ config(
    materialized="table",
    tags=['marts','dimension']
) }}

with streaming_rel as (

    select
        anime_id,
        streaming_id
    from {{ ref('stg_anime__streaming_rel_anime') }}
),

streaming_base as (
    select
        streaming_id,
        streaming_name
    from {{ ref('stg_anime__streaming') }}
)

select
    g.streaming_id,
    g.streaming_name,
    r.anime_id
from streaming_rel r
join streaming_base g
    on r.streaming_id = g.streaming_id
where r.anime_id is not null
