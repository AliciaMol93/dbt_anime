{{
    config(
        materialized="table",
        unique_key="genre_id",
    )
}}

with
    unnested_genres as (
        select lower(trim(f.value::string)) as genre_name_raw
        from
            {{ source("anime_source", "DETAILS") }} a,
            lateral flatten(input => parse_json(a.genres)) f

        where
            a.genres is not null
            and f.value::string <> '[]'
            and f.value::string is not null
    ),

    -- 2. Identificar los valores únicos y generar la clave subrogada
    distinct_genres as (
        select distinct
            {{ surrogate_key(["genre_name_raw"]) }} as genre_id,

            genre_name_raw as genre_name

        from unnested_genres
        where genre_name_raw is not null
    )

select *
from
    distinct_genres
    -- NOTA: Quita la lógica incremental por ahora para que solo se ejecute como TABLE
    
