{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_energyprov AS (
    SELECT DISTINCT
        MD5(energy_provider_name) AS energy_provider_id
        , energy_provider_name
    FROM base
    )
SELECT * FROM silver_energyprov