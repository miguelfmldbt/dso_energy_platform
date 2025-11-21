WITH stg_selfconsumption AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumption') }}
    ),
silver_consumertype AS (
    SELECT DISTINCT
        MD5(consumer_type) AS consumer_type_id
        , CAST(consumer_type:consumer_type_desc AS VARCHAR) AS cons_type_desc
        , data_ingest
    FROM stg_selfconsumption
    )
SELECT * FROM silver_consumertype