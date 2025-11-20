{{ config(materialized="incremental", unique_key="anime_id", on_schema_change="fail") }}

with
    details_clean as (
        select
            trim(title) as raw_title,
            trim(title_japanese) as raw_title_japanese,

            cast({{ surrogate_key(["mal_id"]) }} as varchar) as anime_id,


            nullif(
                case
                    when raw_title in ('', 'null', 'NULL')
                    then null
                    when right(raw_title, 1) in ('.', ',')
                    then left(raw_title, length(raw_title) - 1)
                    else raw_title
                end,
                ''
            ) as title,

            nullif(
                case
                    when raw_title_japanese in ('', 'null', 'NULL')
                    then null
                    when right(raw_title_japanese, 1) in ('.', ',')
                    then left(raw_title_japanese, length(raw_title_japanese) - 1)
                    else raw_title_japanese
                end,
                ''
            ) as title_japanese,

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

            current_timestamp() as ingestion_ts

        from {{ source("anime_source", "DETAILS") }}
    ),

    -- Si un anime_id aparece varias veces cogemos el registro más reciente 
    dedup as (
        select
            *,
            row_number() over (partition by anime_id order by ingestion_ts desc) as rn
        from details_clean
    ),

    final_records as (
        select
            anime_id,
            title,
            title_japanese,
            type,
            status,
            rank,
            score,
            scored_by,
            popularity,
            members,
            favorites,
            episodes,
            year,
            season,
            source,
            rating,
            demographics,
            url,
            image_url,
            synopsis,
            start_date,
            end_date,
            ingestion_ts
        from dedup
        where rn = 1 and title is not null
    )

-- Insertamos solo nuevos animes que no existen
select *
from final_records f
{% if is_incremental() %}
    where not exists (select 1 from {{ this }} t where t.anime_id = f.anime_id)
{% endif %}
