{{ config(
    materialized='incremental',
    unique_key='anime_id',
    on_schema_change='sync_all_columns'
) }}

with anime_titles as (
    select
        cast({{ surrogate_key(['mal_id']) }} as varchar) as anime_id,
        trim(title) as title,
        trim(title_japanese) as title_japanese,
        current_timestamp() as title_ingestion_ts
    from {{ ref('stg_anime__anime') }}
),

anime_details as (
    select
        cast({{ surrogate_key(['mal_id']) }} as varchar) as anime_id,
        lower(trim(type)) as type,
        lower(trim(status)) as status,
        episodes,
        year,
        lower(trim(season)) as season,
        lower(trim(source)) as source,
        lower(trim(rating)) as rating,
        lower(trim(demographics)) as demographics,
        start_date,
        end_date,
        case when url rlike '^https?://[^\s]+$' then url else null end as url,
        case when image_url rlike '^https?://[^\s]+$' then image_url else null end as image_url,
        synopsis,
        current_timestamp() as details_ingestion_ts
    from {{ ref('stg_anime__details') }}
)

select
    t.anime_id,
    t.title,
    t.title_japanese,
    d.type,
    d.status,
    d.episodes,
    d.year,
    d.season,
    d.source,
    d.rating,
    d.demographics,
    d.start_date,
    d.end_date,
    d.url,
    d.image_url,
    d.synopsis,
    greatest(t.title_ingestion_ts, d.details_ingestion_ts) as last_updated
from anime_titles t
left join anime_details d
    on t.anime_id = d.anime_id

{% if is_incremental() %}
where t.title_ingestion_ts > (select max(title_ingestion_ts) from {{ this }})
   or d.details_ingestion_ts > (select max(details_ingestion_ts) from {{ this }})
{% endif %}
