WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_centers AS (
    SELECT DISTINCT
    MD5(ctCode || '|' || distributorId) AS ct_id
    , distributorId
    , ctCode
    , ctName 
    , data_ingest 
    FROM base
)

SELECT *
FROM silver_centers