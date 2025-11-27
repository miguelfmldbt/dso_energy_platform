{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}
    ),
silver_configtype AS (
    SELECT DISTINCT
        MD5(configuration_type || '|' || conf_type_desc) AS config_id
        , configuration_type
        , conf_type_desc
    FROM base
    )
SELECT * FROM silver_configtype