{{
    config(
        materialized='incremental',
        unique_key = 'quality_event_id',
        on_schema_change='fail'
    )
}}

WITH src_s09 AS (
    SELECT * 
    FROM {{ source('s09_data', 's09_raw_data') }}

    {% if is_incremental() %}

    where data_ingest > (select max(data_ingest) from {{ this }})

    {% endif %}

    ),
silver_s09 AS (
    SELECT
        MD5(Cnt || '|' || LEFT(FH,17) || '|' || dso || '|' || data_ingest) AS quality_event_id
        , MD5(Cnt || '|' || dso) AS meter_id
        , CAST(Cnc AS VARCHAR) AS concentrator
        , CONVERT_TIMEZONE('Europe/Madrid','UTC',TO_TIMESTAMP(LEFT(FH, LENGTH(FH) - 1),'YYYYMMDDHH24MISSFF3')) AS quality_event_timestamp
        , CAST(Et AS INTEGER) AS quality_event_group
        , CASE
            WHEN C IN ()
        , CONVERT_TIMEZONE('Europe/Madrid','UTC',TO_TIMESTAMP(LEFT(FH, LENGTH(D1) - 1),'YYYYMMDDHH24MISSFF3')) AS quality_event_finish
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM silver_s09
    )
SELECT * FROM base_s09