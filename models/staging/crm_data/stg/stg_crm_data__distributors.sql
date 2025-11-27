{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_distributor AS (
    SELECT DISTINCT
    dso_id
    , dso_name
    FROM base
)

SELECT *
FROM silver_distributor