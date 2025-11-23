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
        MD5(solar_zone) AS solar_zone_id
        , CAST(solar_zone:solar_zone_name AS VARCHAR) AS solar_zone_name
        , CAST(solar_zone:solar_zone_num AS INTEGER) AS solar_zone_number
    FROM base
    )
SELECT * FROM silver_solarzone