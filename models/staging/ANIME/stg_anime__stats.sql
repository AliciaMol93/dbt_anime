{{ config(materialized="table", unique_key="anime_id") }}

with
    source_stats as (
        select
            {{ surrogate_key(["MAL_ID"]) }} as anime_id,
            try_cast(watching as int) as watching_count,
            try_cast(completed as int) as completed_count,
            try_cast(on_hold as int) as on_hold_count,
            try_cast(dropped as int) as dropped_count,
            try_cast(plan_to_watch as int) as plan_to_watch_count,
            try_cast(total as int) as total_users_interacting_count,
            current_timestamp() as last_updated_at
        from {{ source('anime_source', 'STATS') }}
)
select * from source_stats

