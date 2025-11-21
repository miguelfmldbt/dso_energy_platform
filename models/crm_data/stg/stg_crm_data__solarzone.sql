WITH stg_selfconsumption AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumption') }}
    ),
silver_solarzone AS (
    SELECT DISTINCT
        MD5(solar_zone) AS solar_zone_id
        , CAST(solar_zone:solar_zone_name AS VARCHAR) AS solar_zone_name
        , CAST(solar_zone:solar_zone_num AS VARCHAR) AS solar_zone_num
        , data_ingest
    FROM stg_selfconsumption
    )
SELECT * FROM silver_solarzone