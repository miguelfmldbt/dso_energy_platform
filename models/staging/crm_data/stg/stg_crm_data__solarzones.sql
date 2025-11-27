{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}
    ),
silver_solarzone AS (
    SELECT DISTINCT
        solar_zone_id
        , solar_zone_name
        , solar_zone_number
    FROM base
    )
SELECT * FROM silver_solarzone