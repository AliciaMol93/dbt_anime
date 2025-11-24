{{ config(materialized="table", unique_key="anime_id") }}

with
    source_stats as (
        select
            cast({{ surrogate_key(["mal_id"]) }} as string) as anime_id,
            try_cast(watching as int) as watching_raw,
            try_cast(completed as int) as completed_raw,
            try_cast(on_hold as int) as on_hold_raw,
            try_cast(dropped as int) as dropped_raw,
            try_cast(plan_to_watch as int) as ptw_raw,
            try_cast(total as int) as total_raw,
            ingestion_ts
        from {{ source("anime_source", "STATS") }}
    ),

    validated as (
        select *, completed_raw / nullif(watching_raw, 0) as completion_rate
        from source_stats
    ),

    cleaned as (
        select
            anime_id,
            ingestion_ts,

            -- valores negativos a cero
            greatest(watching_raw, 0) as watching_count,

            -- corregir completados imposibles
            case
                when watching_raw > 100000 and completed_raw < 50
                then 0
                when completion_rate < 0.0001
                then 0
                else greatest(completed_raw, 0)
            end as completed_count,

            greatest(on_hold_raw, 0) as on_hold_count,
            greatest(dropped_raw, 0) as dropped_count,
            greatest(ptw_raw, 0) as plan_to_watch_count,
            greatest(total_raw, 0) as total_users_interacting_count
        from validated
    )

select *
from cleaned
