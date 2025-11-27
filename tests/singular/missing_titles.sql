-- tests/singular/missing_titles.sql

select *
from {{ ref('stg_anime__titles') }}
where (title_clean is null or trim(title_clean) = '')
  and (title_japanese is null or trim(title_japanese) = '')
