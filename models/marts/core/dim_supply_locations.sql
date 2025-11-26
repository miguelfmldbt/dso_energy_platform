{{
    config(
        materialized='incremental',
        unique_key = 'supplyPoint',
        on_schema_change='fail'
    )
}}

WITH stg_meters AS (
    SELECT
        meter_supply_id,
        meter_id
    FROM {{ ref('stg_crm_data__meters') }}
),

stg_supplypoints AS (
    SELECT
        supplyPoint,
        meter_supply_id
    FROM {{ ref('stg_crm_data__supply_points') }}

    {% if is_incremental() %}
    WHERE supplyPoint NOT IN (SELECT supplyPoint FROM {{ this }})
    {% endif %}
),

stg_geograph AS (
    SELECT 
        supplyPoint,
        x_coord,
        y_coord,
        z_coord,
        ST_Y(ST_TRANSFORM(TO_GEOMETRY('POINT(' || x_coord || ' ' || y_coord || ')', 25830),4326)) AS latitude,
        ST_X(ST_TRANSFORM(TO_GEOMETRY('POINT(' || x_coord || ' ' || y_coord || ')', 25830),4326)) AS longitude
    FROM {{ ref('stg_crm_data__geograph_points') }}
),

dim_supply_locations AS (
    SELECT
        MD5(
            stg_meters.meter_supply_id || '|' ||
            stg_meters.meter_id || '|' ||
            stg_geograph.x_coord || '|' ||
            stg_geograph.y_coord || '|' ||
            stg_geograph.z_coord
        ) AS dim_supply_locations_id,
        stg_meters.meter_id,
        stg_geograph.x_coord,
        stg_geograph.y_coord,
        stg_geograph.z_coord,
        stg_geograph.latitude,
        stg_geograph.longitude
    FROM stg_supplypoints
    LEFT JOIN stg_meters
        ON stg_supplypoints.meter_supply_id = stg_meters.meter_supply_id
    LEFT JOIN stg_geograph
        ON stg_supplypoints.supplyPoint = stg_geograph.supplyPoint
)

SELECT *
FROM dim_supply_locations