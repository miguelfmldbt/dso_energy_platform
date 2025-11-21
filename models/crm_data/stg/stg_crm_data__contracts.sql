WITH stg_contracts AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_contracts AS (
    SELECT
        id AS contract_id
        , MD5(contractedPower) AS contractedPowerID
        , supplyPoint
        , MD5(economicActivity) AS economicActivityID
        , end_date
        , MD5(energyProviderName) AS energyProviderID
        , init_date
        , MD5(responsibleEnergyContract) AS responsibleEnergyContractID
        , contract_status
        , tariff
        , tensionLevelMeasurePoint
        , data_ingest
    FROM stg_contracts
    )
SELECT * FROM silver_contracts