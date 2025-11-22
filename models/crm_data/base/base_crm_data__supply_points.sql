WITH src_supplypoints AS (
    SELECT * 
    FROM {{ source('crm_data', 'supply_points') }}
    ),
renamed_casted AS (
    SELECT
        cups AS supplyPoint_id
        , address
        , COALESCE(NULLIF(TRIM(ctCode), ''), 'Unknown') AS ctCode
        , COALESCE(NULLIF(TRIM(ctName), ''), 'Unknown') AS ctName
        , COALESCE(NULLIF(TRIM(trafoCode), ''), 'Unknown') AS trafoCode
        , COALESCE(
            NULLIF(
                IFF(
                    REGEXP_LIKE(lineCode, '^[0-9]+$'), 
                    LPAD(TO_NUMBER(lineCode), 1, '0'),
                    lineCode
                ),
                ''
            ),
            'Unknown'
        ) AS lineCode
        , distributorId
        , distributorCdos
        , measureTensionLevel
        , CAST(tensionSection AS FLOAT) AS tensionSection
        , meterSerialNumber AS meter_id
        , meterTechCode
        , meterTechName
        , COALESCE(NULLIF(phase, ''), 'Unknown') AS phase
        , COALESCE(NULLIF(pointType, ''), 'Unknown') AS pointType
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_supplypoints
    )
SELECT * FROM renamed_casted