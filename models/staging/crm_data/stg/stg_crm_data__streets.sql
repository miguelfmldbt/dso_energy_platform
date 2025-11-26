{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_street AS (
    SELECT DISTINCT
        MD5(street_name || '|' || postal_code || '|' || town_name || '|' || municipality_name || '|' || municipality_ine_code || '|' || province_name || '|' || province_ine_code) AS street_id
        , MD5(town_name || '|' || municipality_name || '|' || municipality_ine_code || '|' || province_name || '|' || province_ine_code) AS town_id
        , postal_code
        , street_name
    FROM base
)
SELECT *
FROM silver_street

