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
        cnmc_type_id
        , cnmc_type_desc
        , cnmc_type_name
    FROM base
    )
SELECT * FROM silver_cnmc_solar