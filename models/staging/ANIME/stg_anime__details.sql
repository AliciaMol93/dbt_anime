{{ config(materialized="incremental") }}

with
    details_clean as (
        select
            cast({{ surrogate_key(["mal_id"]) }} as varchar) as anime_id,
            nullif(lower(trim(type)), '') as type,
            nullif(lower(trim(status)), '') as status,
            rank,
            score,
            scored_by,
            popularity,
            members,
            favorites,
            episodes,
            year,
            nullif(lower(trim(season)), '') as season,
            nullif(lower(trim(source)), '') as source,
            nullif(lower(trim(rating)), '') as rating,
            nullif(lower(trim(demographics)), '') as demographics,

            case when url rlike '^https?://[^\s]+$' then url else null end as url,
            case
                when image_url rlike '^https?://[^\s]+$' then image_url else null
            end as image_url,

            synopsis,
            start_date,
            end_date,
            ingestion_ts

        from {{ source("anime_source", "DETAILS") }}
    )

select * from details_clean
{% if is_incremental() %}
where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
