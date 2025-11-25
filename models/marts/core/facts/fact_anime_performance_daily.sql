{{ config(
    materialized="incremental",
    unique_key=["anime_id", "date_id"],
    incremental_strategy="merge",
    on_schema_change="sync_all_columns"
) }}

with

-- Scores históricos (media ponderada REAL)
scores as (
    select
        anime_id,
        sum(score * votes) / nullif(sum(votes), 0) as avg_score,
        sum(votes) as total_votes,
        stddev_pop(score) as score_dispersion
    from {{ ref('stg_anime__scores') }}
    group by anime_id
),

-- Engagement diario
daily_stats as (
    select
        anime_id,
        total_users_interacting_count as total_engagement,
        watching_count,
        completed_count,
        dropped_count,
        on_hold_count,
        plan_to_watch_count,
        cast(ingestion_ts as date) as stat_date
    from {{ ref('stg_anime__stats') }}
),

-- 3️⃣ Dim date join
daily_with_date as (
    select
        d.*,
        dd.date_id
    from daily_stats d
    left join {{ ref('dim_anime_date') }} dd
        on d.stat_date = dd.date
),

-- 4️⃣ Cálculo de métricas avanzadas
final_fact as (
    select
        d.anime_id,
        d.date_id,
        d.stat_date,

        -- Calidad del anime (basada en votos reales)
        s.avg_score,
        s.total_votes,
        s.score_dispersion,

        -- Engagement diario
        d.total_engagement,
        d.watching_count,
        d.completed_count,
        d.dropped_count,
        d.on_hold_count,
        d.plan_to_watch_count,

        -- Índice combinado de éxito diario
        (coalesce(s.avg_score, 0) * ln(1 + coalesce(d.total_engagement, 0))) as success_index,

        -- Métricas adicionales útiles para análisis
        case 
            when watching_count + completed_count + dropped_count 
                 + on_hold_count + plan_to_watch_count = 0 then null
            else completed_count / nullif(total_engagement,0)
        end as completion_ratio,

        case 
            when watching_count = 0 then null
            else dropped_count / nullif(watching_count,0)
        end as drop_rate
    from daily_with_date d
    left join scores s on d.anime_id = s.anime_id
)

select * from final_fact

{% if is_incremental() %}
    where date_id > (select coalesce(max(date_id), 0) from {{ this }})
{% endif %}
