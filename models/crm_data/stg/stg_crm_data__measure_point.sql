WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_measure_point AS (
    SELECT DISTINCT
        MD5(measureTensionLevel || '|' || tensionSection) AS measure_point_id
        , measureTensionLevel
        , tensionSection
        , data_ingest
    FROM base
)
SELECT *
FROM silver_measure_point