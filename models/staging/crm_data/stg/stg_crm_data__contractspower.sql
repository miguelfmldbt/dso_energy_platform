{{
    config(
        materialized='table'
    )
}}

WITH stg_contracts AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_contractpower AS (
    SELECT DISTINCT
        MD5(contractedPower) AS contracted_power_id
        , CAST(contractedPower:p1 AS INTEGER) AS p1
        , CAST(contractedPower:p2 AS INTEGER) AS p2
        , CAST(contractedPower:p3 AS INTEGER) AS p3
        , CAST(contractedPower:p4 AS INTEGER) AS p4
        , CAST(contractedPower:p5 AS INTEGER) AS p5
        , CAST(contractedPower:p6 AS INTEGER) AS p6
    FROM stg_contracts
    )
SELECT * FROM silver_contractpower