{{
    config(
        materialized='incremental',
        unique_key = 'contract_id',
        on_schema_change='fail'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}

    {% if is_incremental() %}
        WHERE contract_id NOT IN (
            SELECT DISTINCT contract_id
            FROM {{ this }}
        )
    {% endif %}

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
    FROM base
    )
SELECT * FROM silver_contracts