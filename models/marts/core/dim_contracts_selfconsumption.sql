{{ 
    config(
        materialized='table',
    )
}}

WITH stg_contracts AS (
    SELECT
        supplyPoint,
        tariff_id,
        energy_provider_id
    FROM {{ ref('stg_crm_data__contracts') }}
),
stg_tariffs AS (
    SELECT
        tariff_id,
        tariff
    FROM {{ ref('stg_crm_data__tariffs') }}
),
stg_energyproviders AS (
    SELECT 
        energy_provider_id,
        energy_provider_name
    FROM {{ ref('stg_crm_data__energyproviders') }}
),
stg_selfconsumptions AS (
    SELECT
        supplyPoint,
        consumer_type_id,
        generation_pot
    FROM {{ ref('stg_crm_data__selfconsumptions') }}
),
stg_consumertypes AS (
    SELECT
        consumer_type_id,
        consumer_type_desc
    FROM {{ ref('stg_crm_data__consumertypes') }}
),
stg_supplypoints AS (
    SELECT
        supplyPoint,
        meter_supply_id
    FROM {{ ref('stg_crm_data__supply_points') }}
),
stg_meters AS (
    SELECT
        meter_supply_id,
        meter_id
    FROM {{ ref('stg_crm_data__meters') }}
),
contracts_cte AS (
    SELECT
        stg_contracts.supplyPoint,
        stg_tariffs.tariff,
        stg_energyproviders.energy_provider_name
    FROM stg_contracts
    LEFT JOIN stg_tariffs
    ON stg_contracts.tariff_id = stg_tariffs.tariff_id
    LEFT JOIN stg_energyproviders
    ON stg_contracts.energy_provider_id = stg_energyproviders.energy_provider_id
),
selfconsumptions_cte AS (
    SELECT
        stg_selfconsumptions.supplyPoint,
        stg_consumertypes.consumer_type_desc,
        stg_selfconsumptions.generation_pot
    FROM stg_selfconsumptions
    LEFT JOIN stg_consumertypes
    ON stg_selfconsumptions.consumer_type_id = stg_consumertypes.consumer_type_id
),
supply_points_cte AS (
    SELECT 
        stg_supplypoints.supplyPoint,
        stg_meters.meter_id
    FROM stg_supplypoints
    LEFT JOIN stg_meters
    ON stg_supplypoints.meter_supply_id = stg_meters.meter_supply_id 
),
dim_contracts_selfconsumption AS (
    SELECT
        MD5(
            contracts_cte.supplyPoint|| '|' ||
            supply_points_cte.meter_id || '|' ||
            contracts_cte.tariff || '|' ||
            contracts_cte.energy_provider_name || '|' ||
            consumer_type_desc || '|' ||
            CAST(generation_pot AS VARCHAR)
        ) AS dim_contracts_selfconsumption_id,
        contracts_cte.supplyPoint AS supplyPoint,
        supply_points_cte.meter_id AS meter_id,
        contracts_cte.tariff AS tariff,
        contracts_cte.energy_provider_name AS energy_provider_name,
        COALESCE(selfconsumptions_cte.consumer_type_desc, 'no_selfconsumption') AS consumer_type_desc,
        COALESCE(selfconsumptions_cte.generation_pot, 0) AS generation_pot
    FROM contracts_cte
    LEFT JOIN supply_points_cte
        ON contracts_cte.supplyPoint = supply_points_cte.supplyPoint
    LEFT JOIN selfconsumptions_cte
        ON contracts_cte.supplyPoint = selfconsumptions_cte.supplyPoint
)
SELECT *
FROM dim_contracts_selfconsumption