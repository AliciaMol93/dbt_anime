{{ 
    config(
        materialized="incremental",
        unique_key="anime_id",
        incremental_strategy="merge",
        on_schema_change="fail"
    )
}}

with raw as (
    select *
    from {{ source("anime_source", "DETAILS") }}
),

dedup as (
    select *
    from raw
    qualify row_number() over (
        partition by MAL_ID
        order by INGESTION_TS desc
    ) = 1
),

details_clean as (
    select
        {{ surrogate_key(['MAL_ID']) }} as anime_id,
        MAL_ID,
        nullif(lower(trim(type)), '') as type,
        nullif(lower(trim(status)), '') as status,
        rank,
        score,
        scored_by,
        popularity,
        members,
        favorites,
        cast(episodes as int) as episodes,
        cast(year as int) as year,
        nullif(lower(trim(season)), '') as season,
        nullif(lower(trim(source)), '') as source,
        nullif(lower(trim(rating)), '') as rating,
        nullif(lower(trim(demographics)), '') as demographics,

        case when url rlike '^https?://[^\s]+$' then url else null end as url,
        case when image_url rlike '^https?://[^\s]+$' then image_url else null end as image_url,

        synopsis,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        ingestion_ts
    from dedup
)

select *
from details_clean

{% if is_incremental() %}
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
