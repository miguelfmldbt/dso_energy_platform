WITH base_contracts AS (
    SELECT * 
    FROM {{ source('crm_data', 'contracts') }}
    ),
renamed_casted AS (
    SELECT
        id AS contract_id
        , contractedPower
        , cups AS supplyPoint
        , distributorId AS distributorId
        , economicActivity
        , CONVERT_TIMEZONE('UTC', endDate) AS endDate
        , energyProviderName AS energyProviderName
        , responsibleEnergyContract
        , servicePointMunicipality AS Municipality
        , CAST(servicePointZipCode AS INTEGER) AS ZipCode
        , status AS contract_status
        , tariff
        , tensionLevelMeasurePoint
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM base_contracts
    )
SELECT * FROM renamed_casted