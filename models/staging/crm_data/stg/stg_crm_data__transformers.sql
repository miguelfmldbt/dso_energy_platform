{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_transformer AS (
    SELECT DISTINCT
        MD5(transformer_code || '|' || ct_code || '|' || dso_id) AS transformer_id
        , MD5(ct_code || '|' || dso_id) AS ct_id
        , transformer_code
    FROM base
)
SELECT * FROM silver_transformer
