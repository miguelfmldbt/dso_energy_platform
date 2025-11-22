WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_line AS (
    SELECT DISTINCT
        MD5(lineCode || '|' || trafoCode || '|' || ctCode || '|' || distributorId) AS line_id
        , MD5(trafoCode || '|' || ctCode || '|' || distributorId) AS transformer_id
        , lineCode
        , data_ingest
    FROM base
)
SELECT *
FROM silver_line