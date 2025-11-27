{{
    config(
        materialized='incremental',
        unique_key = 'energy_id',
        on_schema_change='fail'
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
        meter_supply_id
    FROM {{ ref('stg_crm_data__supply_points') }}
),

stg_meters AS (
    SELECT
        meter_supply_id,
        meter_id
    FROM {{ ref('stg_crm_data__meters') }}
),

master_supply_contracts_meters AS (
    SELECT 
        stg_contracts.supplyPoint,
        stg_meters.meter_id
    FROM stg_contracts
    LEFT JOIN stg_supplypoints
        ON stg_contracts.supplyPoint = stg_supplypoints.supplyPoint
    LEFT JOIN stg_meters
        ON stg_supplypoints.meter_supply_id = stg_meters.meter_supply_id    
),

fct_energy AS (
    SELECT
    energy_id
    , meter_id
    , concentrator
    , MD5(CAST(energy_timestamp AS DATE)) AS date_id
    , energy_timestamp
    , (active_imported_energy_Wh - active_exported_energy_Wh) AS total_energy_Wh
    FROM {{ ref('stg_s02_data__s02') }}

    {% if is_incremental() %}
    WHERE energy_id NOT IN (SELECT energy_id FROM {{ this }})
    {% endif %}
),

fct_energy_final AS (
    SELECT
        fct_energy.energy_id,
        master_supply_contracts_meters.supplyPoint,
        fct_energy.meter_id,
        fct_energy.concentrator,
        fct_energy.date_id,
        fct_energy.energy_timestamp,
        fct_energy.total_energy_Wh
    FROM fct_energy
    LEFT JOIN master_supply_contracts_meters
        ON fct_energy.meter_id = master_supply_contracts_meters.meter_id
    WHERE master_supply_contracts_meters.supplyPoint IS NOT NULL       
)
SELECT *
FROM fct_energy_final