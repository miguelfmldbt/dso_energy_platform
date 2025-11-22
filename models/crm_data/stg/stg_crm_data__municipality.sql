WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
clean AS (
    SELECT DISTINCT
    COALESCE(NULLIF(INITCAP(TRIM(CAST(address:province AS VARCHAR))), ''),'Unknown') AS province_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:provinceIneCode  AS VARCHAR))), ''),'Unknown') AS province_ine_code
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:municipality AS VARCHAR))), ''),'Unknown') AS municipality_name 
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:municipalityIneCode AS VARCHAR))), ''),'Unknown') AS municipality_ine_code
    , data_ingest             
),
silver_municipality AS (
    SELECT
    MD5(UPPER(municipality_name || '|' || municipality_ine_code)) AS municipality_id
    , MD5(UPPER(province_name || '|' || province_ine_code)) AS province_id
    , municipality_name
    , municipality_ine_code 
    , data_ingest 
    FROM clean
)

SELECT *
FROM silver_municipality
