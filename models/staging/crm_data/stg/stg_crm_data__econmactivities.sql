{{
    config(
        materialized='table'
    )
}}
WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_econactivity AS (
    SELECT DISTINCT
        MD5(economicActivity) AS economic_activity_id
        , CAST(economicActivity:cnae AS INTEGER) AS cnae
        , CAST(economicActivity:cnaeDescription AS VARCHAR) AS cnae_desc
        , CAST(economicActivity:group AS VARCHAR) AS cnae_group
    FROM base
    )
SELECT * FROM silver_econactivity