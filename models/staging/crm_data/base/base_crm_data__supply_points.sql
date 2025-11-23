{{
    config(
        materialized='incremental',
        unique_key = 'supplyPoint',
        on_schema_change='fail'
    )
}}

WITH src_supplypoints AS (
    SELECT * 
    FROM {{ source('crm_data', 'supply_points') }}

    {% if is_incremental() %}

    where data_ingest > (select max(data_ingest) from {{ this }})

    {% endif %}

    ),
renamed_casted AS (
    SELECT
        cups AS supplyPoint
        , address
        , COALESCE(NULLIF(TRIM(ctCode), ''), 'Unknown') AS ct_code
        , COALESCE(NULLIF(TRIM(ctName), ''), 'Unknown') AS ct_name
        , COALESCE(NULLIF(TRIM(trafoCode), ''), 'Unknown') AS transformer_code
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
        ) AS line_code
        , CAST(distributorId AS VARCHAR) AS dso_id
        , CAST(distributorCdos AS VARCHAR) AS dso_name
        , CAST(measureTensionLevel AS VARCHAR) AS measure_tension_level
        , CAST(tensionSection AS FLOAT) AS tension_section
        ,COALESCE(NULLIF(meterSerialNumber, ''), 'Unknown') AS meter_code
        , CAST(meterTechCode AS VARCHAR) AS meter_tech_code
        , CAST(meterTechName AS VARCHAR) AS meter_tech_name
        , COALESCE(NULLIF(phase, ''), 'Unknown') AS phase
        , COALESCE(NULLIF(pointType, ''), 'Unknown') AS point_type
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_supplypoints
    )
SELECT * FROM renamed_casted