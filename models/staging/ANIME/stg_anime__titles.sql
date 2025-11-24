{{ 
    config(
        materialized="incremental",
        unique_key="anime_id",
        on_schema_change="fail",
        incremental_strategy="merge"
    ) 
}}

with title_anime as (
    select
        cast(md5(cast(coalesce(cast(mal_id as text), '') as text)) as varchar) as anime_id,

        nullif(
            case
                when trim(title) in ('', 'NULL', 'null') then null
                when right(trim(title), 1) in ('.', ',') then left(trim(title), length(trim(title)) - 1)
                else trim(title)
            end,
            ''
        ) as title_clean,

        nullif(
            case
                when trim(title_japanese) in ('', 'NULL', 'null') then null
                when right(trim(title_japanese), 1) in ('.', ',') then left(trim(title_japanese), length(trim(title_japanese)) - 1)
                else trim(title_japanese)
            end,
            ''
        ) as title_japanese,

        current_timestamp() as ingestion_ts
    from {{ source("anime_source", "DETAILS") }}
),

deduped as (
    select anime_id, title_clean, title_japanese, ingestion_ts
    from (
        select *,
            row_number() over(partition by title_clean, title_japanese order by ingestion_ts desc) as rn
        from title_anime
    ) t
    where rn = 1
)

select *
from deduped
{% if is_incremental() %}
    where anime_id not in (select anime_id from {{ this }})
{% endif %}
