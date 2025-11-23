{{ 
    config(
        materialized='incremental',
        unique_key='energy_id',
        on_schema_change='fail'
    )
}}

WITH src AS (
    SELECT *
    FROM {{ source('s02_data', 's02_raw_data') }}

    {% if is_incremental() %}
        WHERE data_ingest > (SELECT max(data_ingest) FROM {{ this }})
    {% endif %}
),

-- Deduplicación dentro del propio batch
row_cte AS (
    SELECT
        Cnt,
        dso,
        Cnc,
        FH,
        AE, AI, R1, R2, R3, R4, Magn,
        data_ingest,
        ROW_NUMBER() OVER (
            PARTITION BY Cnt, dso, LEFT(FH,17)
            ORDER BY data_ingest DESC
        ) AS rn
    FROM src
),

silver_s02 AS (
    SELECT
        MD5(Cnt || '|' || LEFT(FH,17) || '|' || dso) AS energy_id,
        MD5(Cnt || '|' || dso) AS meter_id,
        CAST(Cnc AS VARCHAR) AS concentrator,
        CONVERT_TIMEZONE('Europe/Madrid', 'UTC', TO_TIMESTAMP(LEFT(FH,14), 'YYYYMMDDHH24MISS')) AS energy_timestamp,
        CAST(AE AS FLOAT) * CAST(Magn AS INTEGER) AS active_exported_energy_Wh,
        CAST(AI AS FLOAT) * CAST(Magn AS INTEGER) AS active_imported_energy_Wh,
        CAST(R1 AS FLOAT) * CAST(Magn AS INTEGER) AS reactive_energy_q1_VAr,
        CAST(R2 AS FLOAT) * CAST(Magn AS INTEGER) AS reactive_energy_q2_VAr,
        CAST(R3 AS FLOAT) * CAST(Magn AS INTEGER) AS reactive_energy_q3_VAr,
        CAST(R4 AS FLOAT) * CAST(Magn AS INTEGER) AS reactive_energy_q4_VAr,
        CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM row_cte
    WHERE rn = 1 --clean duplicates
)

SELECT * FROM silver_s02
