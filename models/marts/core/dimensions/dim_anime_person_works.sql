{{ config(materialized="table", unique_key=["person_id", "anime_id", "role"]) }}

with
    src as (
        select person_id, anime_id, lower(trim(position)) as raw_role
        from {{ ref("stg_anime__person_anime_works") }}
        where person_id is not null and anime_id is not null
    ),

    clean_base as (
        select
            person_id,
            anime_id,
            regexp_replace(raw_role, '\\s*\\(.*\\)', '') as role_no_paren
        from src
    ),

    normalized as (
        select
            person_id,
            anime_id,

            case
                when role_no_paren ilike '%director%'
                then 'Directing'
                when
                    role_no_paren ilike '%script%'
                    or role_no_paren ilike '%screenplay%'
                    or role_no_paren ilike '%composition%'
                then 'Writing'
                when
                    role_no_paren ilike '%key animation%'
                    or role_no_paren ilike '%in-between%'
                    or role_no_paren ilike '%layout%'
                then 'Animation'
                when
                    role_no_paren ilike '%design%'
                    or role_no_paren ilike '%background%'
                    or role_no_paren ilike '%setting%'
                then 'Design & Art'
                when
                    role_no_paren ilike '%song%'
                    or role_no_paren ilike '%music%'
                    or role_no_paren ilike '%sound%'
                then 'Music'
                when role_no_paren ilike '%original%'
                then 'Original Creator'
                when
                    role_no_paren ilike '%producer%' or role_no_paren ilike '%planning%'
                then 'Production'
                else 'Specialized Technical Roles'
            end as role

        from clean_base
    ),

    dedup as (select distinct person_id, anime_id, role from normalized)

select *
from dedup
