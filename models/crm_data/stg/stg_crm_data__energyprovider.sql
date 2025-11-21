WITH stg_contracts AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_energyprov AS (
    SELECT DISTINCT
        MD5(energyProviderName) AS energyProviderID
        energyProviderName
        , data_ingest
    FROM stg_contracts
    )
SELECT * FROM silver_energyprov