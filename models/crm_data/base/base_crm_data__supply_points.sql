WITH src_supplypoints AS (
    SELECT * 
    FROM {{ source('crm_data', 'supply_points') }}
    ),
renamed_casted AS (
    SELECT
        id AS supplyPoint_id
        , address
        , ctCode
        , ctName
        , trafoCode
        , cups
        , lineCode
        , measureTensionLevel
        , meterSerialNumber AS meter_id
        , meterTechName
        , tensionSection
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_supplypoints
    )
SELECT * FROM renamed_casted