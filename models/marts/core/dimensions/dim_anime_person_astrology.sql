{{ config(
    materialized = "table"
) }}

WITH src AS (
    SELECT
        person_id,
        person_mal_id,
        narrative_about_text
    FROM {{ ref('stg_anime__person_about') }}
    WHERE narrative_about_text IS NOT NULL 
    LIMIT 20000

),

llm_extract AS (
    SELECT
        person_id,
        person_mal_id,
        narrative_about_text,

        AI_EXTRACT(
            text => narrative_about_text,
            responseFormat => 
            ['birthday_date', 'Return the birth date in YYYY-MM-DD format. If the text does not contain a valid date, return NULL.']
        ) AS llm_json

    FROM src
)

--select * from llm_extract
,

parsed AS (
    SELECT
        person_id,
        person_mal_id,
        narrative_about_text,
        llm_json AS j
    FROM llm_extract
),

final_extract AS (
    SELECT
        person_id,
        person_mal_id,
        narrative_about_text,

        TRY_TO_DATE(j:"response":"birthday_date"::varchar)    AS extracted_birthday,
    FROM parsed
)
,

zodiac_calc AS (
    SELECT
        *,
        CASE
            WHEN MONTH(extracted_birthday) = 3 AND DAY(extracted_birthday) >= 21 OR
                 MONTH(extracted_birthday) = 4 AND DAY(extracted_birthday) <= 19 THEN 'Aries'
            WHEN MONTH(extracted_birthday) = 4 AND DAY(extracted_birthday) >= 20 OR
                 MONTH(extracted_birthday) = 5 AND DAY(extracted_birthday) <= 20 THEN 'Taurus'
            WHEN MONTH(extracted_birthday) = 5 AND DAY(extracted_birthday) >= 21 OR
                 MONTH(extracted_birthday) = 6 AND DAY(extracted_birthday) <= 20 THEN 'Gemini'
            WHEN MONTH(extracted_birthday) = 6 AND DAY(extracted_birthday) >= 21 OR
                 MONTH(extracted_birthday) = 7 AND DAY(extracted_birthday) <= 22 THEN 'Cancer'
            WHEN MONTH(extracted_birthday) = 7 AND DAY(extracted_birthday) >= 23 OR
                 MONTH(extracted_birthday) = 8 AND DAY(extracted_birthday) <= 22 THEN 'Leo'
            WHEN MONTH(extracted_birthday) = 8 AND DAY(extracted_birthday) >= 23 OR
                 MONTH(extracted_birthday) = 9 AND DAY(extracted_birthday) <= 22 THEN 'Virgo'
            WHEN MONTH(extracted_birthday) = 9 AND DAY(extracted_birthday) >= 23 OR
                 MONTH(extracted_birthday) = 10 AND DAY(extracted_birthday) <= 22 THEN 'Libra'
            WHEN MONTH(extracted_birthday) = 10 AND DAY(extracted_birthday) >= 23 OR
                 MONTH(extracted_birthday) = 11 AND DAY(extracted_birthday) <= 21 THEN 'Scorpio'
            WHEN MONTH(extracted_birthday) = 11 AND DAY(extracted_birthday) >= 22 OR
                 MONTH(extracted_birthday) = 12 AND DAY(extracted_birthday) <= 21 THEN 'Sagittarius'
            WHEN MONTH(extracted_birthday) = 12 AND DAY(extracted_birthday) >= 22 OR
                 MONTH(extracted_birthday) = 1 AND DAY(extracted_birthday) <= 19 THEN 'Capricorn'
            WHEN MONTH(extracted_birthday) = 1 AND DAY(extracted_birthday) >= 20 OR
                 MONTH(extracted_birthday) = 2 AND DAY(extracted_birthday) <= 18 THEN 'Aquarius'
            WHEN MONTH(extracted_birthday) = 2 AND DAY(extracted_birthday) >= 19 OR
                 MONTH(extracted_birthday) = 3 AND DAY(extracted_birthday) <= 20 THEN 'Pisces'
        END AS zodiac_sign
    FROM final_extract
),

generation_calc AS (
    SELECT
        *,
        CASE
            WHEN YEAR(extracted_birthday) < 1965 THEN 'Boomer'
            WHEN YEAR(extracted_birthday) BETWEEN 1965 AND 1980 THEN 'Gen X'
            WHEN YEAR(extracted_birthday) BETWEEN 1981 AND 1996 THEN 'Millennial'
            WHEN YEAR(extracted_birthday) >= 1997 THEN 'Gen Z'
        END AS generation
    FROM zodiac_calc
)

SELECT *
FROM generation_calc
where extracted_birthday is not null
