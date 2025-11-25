{{ config(
    materialized="table",
    unique_key=["anime_id", "score"]
) }}

with source_stats as (
    select
        cast({{ surrogate_key(["mal_id"]) }} as string) AS anime_id,
        *
    from {{ source("anime_source", "STATS") }}
),

-- 1️⃣ UNPIVOT de votos por score 1-10
votes_unpivot as (
    select
        anime_id,
        to_number(regexp_substr(metric, '\\d+')) as score_raw,
        try_cast(cast(votes as string) as number(38,0)) as votes_raw
    from source_stats
    unpivot (
        votes for metric in (
            SCORE_1_VOTES,
            SCORE_2_VOTES,
            SCORE_3_VOTES,
            SCORE_4_VOTES,
            SCORE_5_VOTES,
            SCORE_6_VOTES,
            SCORE_7_VOTES,
            SCORE_8_VOTES,
            SCORE_9_VOTES,
            SCORE_10_VOTES
        )
    )
),

-- 2️⃣ UNPIVOT de porcentaje 
percentage_unpivot as (
    select
        anime_id,
        to_number(regexp_substr(metric, '\\d+')) as score_raw,
        try_cast(cast(percentage as string) as number(8,5)) as percentage_raw
    from source_stats
    unpivot (
        percentage for metric in (
            SCORE_1_PERCENTAGE,
            SCORE_2_PERCENTAGE,
            SCORE_3_PERCENTAGE,
            SCORE_4_PERCENTAGE,
            SCORE_5_PERCENTAGE,
            SCORE_6_PERCENTAGE,
            SCORE_7_PERCENTAGE,
            SCORE_8_PERCENTAGE,
            SCORE_9_PERCENTAGE,
            SCORE_10_PERCENTAGE
        )
    )
),

scores_clean as (
    select
        v.anime_id,
        cast(v.score_raw as number(2,0)) as score,
        cast(v.votes_raw as number(12,0)) as votes,
        p.percentage_raw as percentage,
        current_timestamp() as ingestion_ts
    from votes_unpivot v
    join percentage_unpivot p
        on v.anime_id = p.anime_id
       and v.score_raw = p.score_raw
    where v.score_raw between 1 and 10
)

select
    anime_id,
    score,
    votes,
    percentage,
    ingestion_ts
from scores_clean
order by anime_id, score
