WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_distributor AS (
    SELECT DISTINCT
    distributorId
    , distributorCdos
    , data_ingest
    FROM base
)

SELECT *
FROM silver_distributor