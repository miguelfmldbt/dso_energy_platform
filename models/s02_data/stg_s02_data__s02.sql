{{
    config(
        materialized='incremental',
        unique_key = 'energy_id',
        on_schema_change='fail'
    )
}}

WITH src_s02 AS (
    SELECT * 
    FROM {{ source('s02_data', 's02_raw_data') }}

    {% if is_incremental() %}

    where data_ingest > (select max(data_ingest) from {{ this }})

    {% endif %}

    ),
silver_s02 AS (
    SELECT
        MD5(Cnt || '|' || LEFT(FH,17) || '|' || dso || '|' || data_ingest) AS energy_id
        , MD5(Cnt || '|' || dso) AS meter_id
        , CAST(Cnc AS VARCHAR) AS concentrator
        , CONVERT_TIMEZONE('Europe/Madrid', 'UTC', TO_TIMESTAMP(LEFT(FH,14), 'YYYYMMDDHH24MISS')) AS energy_timestamp
        , CAST(AE AS FLOAT)*CAST(Magn AS INTEGER) AS active_exported_energy_Wh
        , CAST(AI AS FLOAT)*CAST(Magn AS INTEGER) AS active_imported_energy_Wh
        , CAST(R1 AS FLOAT)*CAST(Magn AS INTEGER) AS reactive_energy_q1_VAr
        , CAST(R2 AS FLOAT)*CAST(Magn AS INTEGER) AS reactive_energy_q2_VAr
        , CAST(R3 AS FLOAT)*CAST(Magn AS INTEGER) AS reactive_energy_q3_VAr
        , CAST(R4 AS FLOAT)*CAST(Magn AS INTEGER) AS reactive_energy_q4_VAr
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_s02
    )
SELECT * FROM silver_s02