WITH stg_contracts AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_contractowner AS (
    SELECT DISTINCT
        MD5(responsibleEnergyContract) AS responsibleEnergyContractID
        , MD5(CAST(responsibleEnergyContract:fiscalId AS VARCHAR)) AS ownerID
        , CAST(responsibleEnergyContract:name AS VARCHAR) AS owner_name
        , data_ingest
    FROM stg_contracts
    )
SELECT * FROM silver_contractowner