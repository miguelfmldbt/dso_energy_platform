
WITH src_contracts AS (
    SELECT * 
    FROM {{ source('crm_data', 'contracts') }}
    ),
renamed_casted AS (
    SELECT
        id AS contract_id
        , contractedPower
        , cups AS supplyPoint
        , economicActivity
        , CONVERT_TIMEZONE('UTC', endDate) AS end_date
        , CAST(energyProviderName AS VARCHAR) AS energy_provider_name
        , responsibleEnergyContract
        , CONVERT_TIMEZONE('UTC', initDate) AS init_date
        , CAST(status AS VARCHAR) AS contract_status
        , CAST(tariff AS VARCHAR) AS tariff
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_contracts
    )
SELECT * FROM renamed_casted