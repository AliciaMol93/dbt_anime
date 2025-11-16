{{ config(
    materialized='table', 
    unique_key=['person_id', 'attribute_key'],
    ) }}

    ¡¡¡¡¡¡CORREGIRRRRRR!!!!!!!!!!!!!!

WITH src_person_details AS (
    SELECT 
        PERSON_MAL_ID, 
        ABOUT
    FROM {{ source('anime_source', 'PERSON_DETAILS') }}
    WHERE ABOUT IS NOT NULL
),

-- 1. División y Filtrado: Convierte el texto en filas y filtra solo CLAVE:VALOR
attributes_raw AS (
    SELECT
        PERSON_MAL_ID,
        -- Divide el texto por comas o saltos de línea. FLATTEN crea las nuevas filas.
        TRIM(value) AS key_value_pair 
    FROM src_person_details,
    -- Reemplaza saltos de línea por comas para usar SPLIT, luego FLATTEN
    LATERAL FLATTEN(INPUT => SPLIT(REPLACE(REPLACE(ABOUT, '\n', ','), ':', ':'), ',')) 
    WHERE key_value_pair LIKE '%:%' -- Aseguramos que tenga el patrón Clave:Valor
),

-- 2. Extracción: Separa explícitamente la clave y el valor
extracted_data AS (
    SELECT
        PERSON_MAL_ID,
        TRIM(SPLIT_PART(key_value_pair, ':', 1)) AS attribute_key_raw,
        TRIM(SPLIT_PART(key_value_pair, ':', 2)) AS attribute_value_raw
        
    FROM attributes_raw
    WHERE attribute_value_raw IS NOT NULL
)

-- 3. SELECT Final: Generación de Clave Subrogada y Estandarización
SELECT
     {{ surrogate_key(['PERSON_MAL_ID']) }} AS person_id,
    
    REPLACE(LOWER(attribute_key_raw), ' ', '_') AS attribute_key,
    LOWER(attribute_value_raw) AS attribute_value

FROM extracted_data

-- **FILTRADO ESTRICTO (LA SOLUCIÓN A TU PROBLEMA)**
-- Solo permitimos las claves que sabemos que son datos analíticos (físicos, geográficos, intereses).
-- Las claves de redes sociales (Pixiv, Twitter, Bluesky, etc.) son descartadas.
WHERE attribute_key_raw IN (
    'Birth name', 'Hometown', 'Birth place', 'Blood type', 'Height', 'Weight', 'Three sizes', 
    'Shoe size', 'Favorites', 'Hobbies', 'Skills', 'Skill/Ability'
)
  AND attribute_value_raw IS NOT NULL