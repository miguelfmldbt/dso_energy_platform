{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_province AS (
    SELECT DISTINCT
    MD5(province_name || '|' || province_ine_code) AS province_id
    , province_name
    , province_ine_code
    FROM base
)

SELECT *
FROM silver_province
