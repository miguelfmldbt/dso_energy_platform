{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}
    ),
silver_consumertype AS (
    SELECT DISTINCT
        MD5(consumer_type) AS consumer_type_id
        , CAST(consumer_type:consumer_type_desc AS VARCHAR) AS consumer_type_desc
    FROM base
    )
SELECT * FROM silver_consumertype