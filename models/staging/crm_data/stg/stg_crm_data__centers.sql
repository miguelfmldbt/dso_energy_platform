{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_centers AS (
    SELECT DISTINCT
    MD5(ct_code || '|' || dso_id) AS ct_id
    , dso_id
    , ct_code
    , ct_name 
    FROM base
)

SELECT *
FROM silver_centers