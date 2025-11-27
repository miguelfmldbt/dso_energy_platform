{{ 
    config(
        materialized='table',
    )
}}

WITH stg_contracts AS (
    SELECT
        supplyPoint
    FROM {{ ref('stg_crm_data__contracts') }}
),

stg_supplypoints AS (
    SELECT
        supplyPoint,
        meter_supply_id,
        address_id
    FROM {{ ref('stg_crm_data__supply_points') }}
),

stg_meters AS (
    SELECT
        meter_supply_id,
        meter_id
    FROM {{ ref('stg_crm_data__meters') }}
),

stg_addresses AS (
    SELECT
        address_id,
        street_id
    FROM {{ ref('stg_crm_data__addresses') }}
),

stg_streets AS (
    SELECT
        street_id,
        town_id
    FROM {{ ref('stg_crm_data__streets') }}
),

stg_towns AS (
    SELECT 
        town_id,
        municipality_id,
        town_name
    FROM {{ ref('stg_crm_data__towns') }}
),

stg_municipalities AS (
    SELECT
        municipality_id,
        province_id,
        municipality_name 
    FROM {{ ref('stg_crm_data__municipalities') }}
),

stg_provinces AS (
    SELECT
        province_id,
        province_name 
    FROM {{ ref('stg_crm_data__provinces') }}
),

dim_addresses AS (
    SELECT
        stg_addresses.address_id AS dim_addresses_id,
        stg_meters.meter_id,
        stg_towns.town_name,
        stg_municipalities.municipality_name,
        stg_provinces.province_name
    FROM stg_contracts
    LEFT JOIN stg_supplypoints
        ON stg_contracts.supplyPoint = stg_supplypoints.supplyPoint

    LEFT JOIN stg_meters
        ON stg_supplypoints.meter_supply_id = stg_meters.meter_supply_id    

    LEFT JOIN stg_addresses
        ON stg_supplypoints.address_id = stg_addresses.address_id

    LEFT JOIN stg_streets
        ON stg_addresses.street_id = stg_streets.street_id

    LEFT JOIN stg_towns
        ON stg_streets.town_id = stg_towns.town_id

    LEFT JOIN stg_municipalities
        ON stg_towns.municipality_id = stg_municipalities.municipality_id

    LEFT JOIN stg_provinces
        ON stg_municipalities.province_id = stg_provinces.province_id    
)
SELECT *
FROM dim_addresses