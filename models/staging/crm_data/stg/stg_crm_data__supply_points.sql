{{ config(
     materialized='table'
    ) 
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__supply_points') }}

    ),
silver_supplypoints AS (
    SELECT
        supplyPoint
        , MD5(address) AS address_id
        , MD5(line_code || '|' || transformer_code || '|' || ct_code || '|' || dso_id) AS line_id
        , MD5(measure_tension_level || '|' || tension_section) AS measure_point_id
        , MD5(dso_name || '|' || meter_code) AS meter_id
        , phase
        , point_type
    FROM base
    )
SELECT * FROM silver_supplypoints