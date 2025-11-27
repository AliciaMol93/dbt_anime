{{ config(materialized="table") }}

with
    base_themes as (
        select
            mal_id,
            f.value::string as theme_name_raw

        from
            {{ source("anime_source", "DETAILS") }} a,
            lateral flatten(input => parse_json(a.themes)) f

        where
            a.themes is not null
            and f.value::string is not null
            and f.value::string <> '[]'
    )

select
    {{ surrogate_key(["mal_id"]) }} as anime_id,
    lower(trim(theme_name_raw)) as theme_name,
    {{ surrogate_key(["lower(trim(theme_name_raw))"]) }} as theme_id
from base_themes
where lower(trim(theme_name_raw)) is not null
