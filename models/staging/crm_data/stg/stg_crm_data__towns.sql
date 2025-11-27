{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
), 
silver_town AS (
    SELECT DISTINCT
    MD5(town_name || '|' || municipality_name || '|' || municipality_ine_code || '|' || province_name || '|' || province_ine_code) AS town_id
    , MD5(municipality_name || '|' || municipality_ine_code || '|' || province_name || '|' || province_ine_code) AS municipality_id
    , town_name
    FROM base
)
SELECT * FROM silver_town
