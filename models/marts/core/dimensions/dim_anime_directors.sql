{{ config(materialized="table") }}

with raw as (
    select
        paw.anime_id,
        paw.person_id,
        pd.name as director_name,
        paw.position
    from {{ ref('stg_anime__person_anime_works') }} paw
    join {{ ref('stg_anime__person_details') }} pd
        on paw.person_id = pd.person_id
    where paw.position ilike '%director%'
),

grouped as (
    select
        anime_id,
        person_id,
        director_name,
        min(position) as sample_role
    from raw
    group by anime_id, person_id, director_name
),

deduplicated as (
    select
        *,
        row_number() over (
            partition by anime_id, person_id
            order by sample_role
        ) as rn
    from grouped
)

select
    anime_id,
    person_id as director_id,
    director_name
from deduplicated
where rn = 1
