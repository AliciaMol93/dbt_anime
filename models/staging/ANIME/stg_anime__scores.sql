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

-- UNPIVOT de VOTES
votes_unpivot as (
    select
        anime_id,
        to_number(regexp_substr(metric, '\\d+')) as score,
        try_cast(cast(votes as string) as NUMBER(38,0)) as votes
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

-- UNPIVOT de PERCENTAGE
percentage_unpivot as (
    select
        anime_id,
        to_number(regexp_substr(metric, '\\d+')) as score,
        percentage
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
)

-- unimos votes + percentage
select
    v.anime_id,
    v.score,
    v.votes,
    p.percentage
from votes_unpivot v
join percentage_unpivot p
    on v.anime_id = p.anime_id
   and v.score = p.score
where v.score between 1 and 10
order by v.anime_id, v.score
