WITH stg_selfconsumption AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumption') }}
    ),
silver_configtype AS (
    SELECT DISTINCT
        MD5(conf_type) AS config_id
        , CAST(conf_type:conf_type AS VARCHAR) AS configuration_type
        , CAST(conf_type:conf_type_desciption AS VARCHAR) AS conf_type_desc
        , data_ingest
    FROM stg_selfconsumption
    )
SELECT * FROM silver_configtype