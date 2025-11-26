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
        MD5(Cnt || '|' || LEFT(FH,17) || '|' || dso) AS quality_event_id
        , MD5(dso || '|' || Cnt) AS meter_id
        , CAST(Cnc AS VARCHAR) AS concentrator
        , CAST(Cnt AS VARCHAR) AS meter_code
        , CONVERT_TIMEZONE('Europe/Madrid','UTC',TO_TIMESTAMP(LEFT(FH, LENGTH(FH) - 1),'YYYYMMDDHH24MISSFF3')) AS quality_event_timestamp
        , CAST(Et AS INTEGER) AS quality_event_group
        , CAST(C AS INTEGER) AS quality_event_type
        , CONVERT_TIMEZONE(
            'Europe/Madrid',
            'UTC',
            CASE 
                WHEN D1 IS NULL OR D1 = '' THEN NULL
                ELSE TO_TIMESTAMP(LEFT(D1, LENGTH(D1) - 1), 'YYYYMMDDHH24MISSFF3')
            END
        ) AS quality_event_start
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_s09
    )
SELECT * FROM silver_s09