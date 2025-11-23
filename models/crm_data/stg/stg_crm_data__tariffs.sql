{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_tariffs AS (
    SELECT DISTINCT
        MD5(tariff) AS tariff_id
        , tariff
    FROM base
    )
SELECT * FROM silver_tariffs