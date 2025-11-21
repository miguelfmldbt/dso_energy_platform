WITH stg_selfconsumption AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumption') }}
    ),
silver_cnmc_solar AS (
    SELECT DISTINCT
        MD5(cnmc_solar) AS cnmc_solar_id
        , CAST(cnmc_solar:cnmc_type_desc AS VARCHAR) AS cnmc_type_desc
        , CAST(cnmc_solar:cnmc_type_name AS INTEGER) AS cnmc_type_name
        , data_ingest
    FROM stg_selfconsumption
    )
SELECT * FROM silver_cnmc_solar