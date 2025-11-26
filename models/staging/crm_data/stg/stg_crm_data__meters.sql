{{
    config(
        materialized='table'
    )
}}
WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_meter AS (
    SELECT DISTINCT
        MD5(dso_name || '|' || meter_code || '|' || supplyPoint) AS meter_supply_id
        , MD5(dso_name || '|' || meter_code) AS meter_id
        , MD5(meter_tech_name) AS meter_tech_id
        , meter_code
    FROM base
)
SELECT *
FROM silver_meter