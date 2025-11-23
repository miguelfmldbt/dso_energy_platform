{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
clean AS (
    SELECT DISTINCT
    COALESCE(NULLIF(INITCAP(TRIM(CAST(address:province AS VARCHAR))), ''),'Unknown') AS province_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:provinceIneCode AS VARCHAR))), ''),'Unknown') AS province_ine_code  
    FROM base    
),
silver_province AS (
    SELECT
    MD5(province_name || '|' || province_ine_code) AS province_id
    , province_name
    , province_ine_code
    FROM clean
)

SELECT *
FROM silver_province
