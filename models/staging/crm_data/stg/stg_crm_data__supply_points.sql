{{
    config(
        materialized='incremental',
        unique_key = 'supplyPoint',
        on_schema_change='fail'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__supply_points') }}

    {% if is_incremental() %}
        WHERE supplyPoint NOT IN (
            SELECT DISTINCT supplyPoint 
            FROM {{ this }}
        )
    {% endif %}

    ),
silver_supplypoints AS (
    SELECT
        supplyPoint
        , MD5(
            street_name || '|' ||
            postal_code || '|' ||
            town_name || '|' ||
            municipality_name || '|' ||
            municipality_ine_code || '|' ||
            province_name || '|' ||
            province_ine_code || '|' ||
            number || '|' ||
            portal || '|' ||
            floor || '|' ||
            letter || '|' ||
            type_address || '|' ||
            phone
            ) AS address_id
        , MD5(line_code || '|' || transformer_code || '|' || ct_code || '|' || dso_id) AS line_id
        , MD5(measure_tension_level || '|' || tension_section) AS measure_point_id
        , MD5(dso_name || '|' || meter_code || '|' || supplyPoint) AS meter_supply_id
        , phase
        , point_type
    FROM base
    )
SELECT * FROM silver_supplypoints