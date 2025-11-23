{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}
    ),
silver_selfconsumption_status AS (
    SELECT DISTINCT
        MD5(selfconsumption_status) AS selfconsumption_status_id
        , CAST(selfconsumption_status:status_name AS VARCHAR) AS status_name
    FROM base
    )
SELECT * FROM silver_selfconsumption_status