-- test: bad_scores_dim.sql
-- Purpose: Detect invalid final score values (0–10) in dim_anime_anime

select *
from {{ ref('dim_anime_anime') }}
where score < 0
   or score > 10