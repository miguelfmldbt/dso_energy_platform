{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_line AS (
    SELECT DISTINCT
        MD5(line_code || '|' || transformer_code || '|' || ct_code || '|' || dso_id) AS line_id
        , MD5(transformer_code || '|' || ct_code || '|' || dso_id) AS transformer_id
        , line_code
    FROM base
)
SELECT *
FROM silver_line