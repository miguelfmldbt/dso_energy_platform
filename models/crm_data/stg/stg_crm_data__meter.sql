WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_meter AS (
    SELECT DISTINCT
        meter_id
        , meterTechCode
        , meterTechName
        , data_ingest
    FROM base
)
SELECT *
FROM silver_meter