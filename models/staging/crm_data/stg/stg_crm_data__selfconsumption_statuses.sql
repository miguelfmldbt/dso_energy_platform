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
        selfconsumption_status_id
        , status_name
    FROM base
    )
SELECT * FROM silver_selfconsumption_status