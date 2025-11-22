WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_transformer AS (
    SELECT DISTINCT
        MD5(trafoCode || '|' || ctCode || '|' || distributorId) AS transformer_id
        , MD5(ctCode || '|' || distributorId) AS ct_id
        , trafoCode
        , data_ingest
    FROM base
)
SELECT * FROM silver_transformer
