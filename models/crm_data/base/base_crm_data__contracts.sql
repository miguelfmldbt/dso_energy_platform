WITH src_contracts AS (
    SELECT * 
    FROM {{ source('crm_data', 'contracts') }}
    ),
renamed_casted AS (
    SELECT
        id AS contract_id
        , contractedPower
        , cups AS supplyPoint
        , distributorId
        , economicActivity
        , CONVERT_TIMEZONE('UTC', endDate) AS end_date
        , energyProviderName
        , responsibleEnergyContract
        , CONVERT_TIMEZONE('UTC', initDate) AS init_date
        , status AS contract_status
        , tariff
        , tensionLevelMeasurePoint
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_contracts
    )
SELECT * FROM renamed_casted