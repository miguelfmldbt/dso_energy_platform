WITH stg_supplypoints AS (
    SELECT * 
    FROM {{ ref('base_crm_data__supply_points') }}
    ),
silver_supplypoints AS (
    SELECT
        supplyPoint_id
        , MD5(address) AS address_id
        , distributorId
        , MD5(measureTensionLevel || '|' || tensionSection) AS measure_point_id
        , meter_id
        , phase
        , pointType
        , data_ingest
    FROM stg_supplypoints
    )
SELECT * FROM silver_supplypoints