{{
    config(
        materialized='table'
    )
}}
WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_meter_tech AS (
    SELECT DISTINCT
        MD5(meter_tech_name) AS meter_tech_id
        , meter_tech_name
        , meter_tech_code
    FROM base
)
SELECT *
FROM silver_meter_tech