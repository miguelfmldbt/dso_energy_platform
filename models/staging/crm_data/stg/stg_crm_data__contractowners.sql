{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_contractowner AS (
    SELECT DISTINCT
        MD5(responsibleEnergyContract) AS contract_owner_id
        , MD5(CAST(responsibleEnergyContract:fiscalId AS VARCHAR)) AS owner_id
        , CAST(responsibleEnergyContract:name AS VARCHAR) AS owner_name
    FROM base
    )
SELECT * FROM silver_contractowner