{{
    config(
        materialized="incremental",
        unique_key="anime_id",
        on_schema_change="fail"
    )
}}

with details_clean as (
    select
        {{ surrogate_key(['mal_id']) }} as anime_id,
        nullif(lower(ltrim(title)), '') as title,
        nullif(lower(ltrim(title_japanese)), '') as title_japanese,

        lower(trim(type)) as type,
        lower(trim(status)) as status,
        rank,
        score,
        scored_by,
        popularity,
        members,
        favorites,
        episodes,
        year,
        trim(season) as season,
        trim(source) as source,
        trim(rating) as rating,
        trim(demographics) as demographics,

        -- URLs válidas (Snowflake/Data Warehouse SQL)
        case when url rlike '^https?://[^\s]+$' then url else null end as url,
        case when image_url rlike '^https?://[^\s]+$' then image_url else null end as image_url,

        synopsis,

        -- Fechas
        start_date,
        end_date,

        current_timestamp() as ingestion_ts 

    from {{ source("anime_source", "DETAILS") }}
),

-- Deduplicación: Si un anime_id aparece varias veces, tomamos el registro más reciente (último ingestion_ts)
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
    where rn = 1
)

-- Lógica Incremental: Insertar solo nuevos animes que no existen en la tabla de destino (Anti-Join)
select *
from final_records f
{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} t
    where t.anime_id = f.anime_id
)
{% endif %}