{{ config(materialized="table", unique_key=["anime_id", "licensor_id"]) }}

with
    unnested_licensors as (
        select
            anime.mal_id as anime_id, lower(trim(f.value::string)) as licensor_name_raw

        from
            {{ source("anime_source", "DETAILS") }} anime,
            lateral flatten(input => parse_json(anime.licensors)) f

        where
            anime.licensors is not null
            and f.value::string is not null
            and f.value::string <> '[]'
    )

-- Resultado Final: Mapear Anime ID a studio ID
select
    t1.anime_id,

    -- Generar la clave foránea de la licencia
    {{ surrogate_key(["t1.licensor_name_raw"]) }} as licensor_id,

    -- Clave primaria compuesta para unicidad
    {{ surrogate_key(["t1.anime_id", "t1.licensor_name_raw"]) }} as anime_licensor_key

from unnested_licensors t1

-- Excluimos valores nulos para asegurar la integridad de la clave
where t1.licensor_name_raw is not null
