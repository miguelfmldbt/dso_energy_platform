{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}
    ),
silver_cnmc_solar AS (
    SELECT DISTINCT
        MD5(cnmc_solar) AS cnmc_solar_id
        , CAST(cnmc_solar:cnmc_type_id AS INTEGER) AS cnmc_type_id
        , CAST(cnmc_solar:cnmc_type_desc AS VARCHAR) AS cnmc_type_desc
        , CAST(cnmc_solar:cnmc_type_name AS INTEGER) AS cnmc_type_name
    FROM base
    )
SELECT * FROM silver_cnmc_solar