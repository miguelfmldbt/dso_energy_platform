{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_municipality AS (
    SELECT DISTINCT
    MD5(municipality_name || '|' || municipality_ine_code || '|' || province_name || '|' || province_ine_code) AS municipality_id
    , MD5(province_name || '|' || province_ine_code) AS province_id
    , municipality_name
    , municipality_ine_code 
    FROM base
)

SELECT *
FROM silver_municipality
