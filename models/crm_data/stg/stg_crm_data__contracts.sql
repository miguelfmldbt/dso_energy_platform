{{
    config(
        materialized='table'
    )
}}
WITH stg_contracts AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}

    ),
silver_contracts AS (
    SELECT
        contract_id
        , MD5(contractedPower) AS contracted_power_id
        , MD5(economicActivity) AS economic_activity_id
        , MD5(energy_provider_name) AS energy_provider_id
        , MD5(responsibleEnergyContract) AS contract_owner_id
        , MD5(tariff) AS tariff_id
        , supplyPoint
        , end_date
        , init_date
        , contract_status
        , tensionLevelMeasurePoint
    FROM stg_contracts
    )
SELECT * FROM silver_contracts