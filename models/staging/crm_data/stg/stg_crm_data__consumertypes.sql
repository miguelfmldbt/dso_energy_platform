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
        consumer_type_id
        , consumer_type_desc
    FROM base
    )
SELECT * FROM silver_consumertype