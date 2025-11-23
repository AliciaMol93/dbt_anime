{{ config(
    materialized='incremental',
    unique_key='anime_id'
) }}

with anime_details as (
    select * from {{ ref('stg_anime__details') }}
),
anime_titles as (
    select * from {{ ref('stg_anime__titles') }}
)

select
    ad.anime_id,
    at.title_clean as title,
    at.title_japanese as title_japanese,
    ad.type,
    ad.status,
    ad.rank,
    ad.score,
    ad.scored_by,
    ad.popularity,
    ad.members,
    ad.favorites,
    ad.episodes,
    ad.year,
    ad.season,
    ad.source,
    ad.rating,
    ad.demographics,
    ad.url,
    ad.image_url,
    ad.synopsis,
    ad.start_date,
    ad.end_date,
    ad.ingestion_ts
from anime_details ad
left join anime_titles at
    on ad.anime_id = at.anime_id


{% if is_incremental() %}
    where ad.ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
