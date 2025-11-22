{{ config(materialized="incremental", unique_key="anime_id", on_schema_change="fail") }}

with
    title_anime_cleaned as (
        select
            cast({{ surrogate_key(["mal_id"]) }} as varchar) as anime_id,
            nullif(
                case
                    when trim(title) in ('', 'null', 'NULL')
                    then null
                    when right(trim(title), 1) in ('.', ',')
                    then left(trim(title), length(trim(title)) - 1)
                    else trim(title)
                end,
                ''
            ) as title,

            nullif(
                case
                    when trim(title_japanese) in ('', 'null', 'NULL')
                    then null
                    when right(trim(title_japanese), 1) in ('.', ',')
                    then left(trim(title_japanese), length(trim(title_japanese)) - 1)
                    else trim(title_japanese)
                end,
                ''
            ) as title_japanese,

            current_timestamp() as ingestion_ts

        from {{ source("anime_source", "DETAILS") }}
    )

select *
from title_anime_cleaned
{% if is_incremental() %}
    where ingestion_ts > (select max(ingestion_ts) from {{ this }})
{% endif %}
