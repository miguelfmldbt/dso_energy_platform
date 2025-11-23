{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}
    ),
silver_selfconsumptions AS (
    SELECT
        REPLACE(TO_VARCHAR(f.value), '"', '') AS supplyPoint
        , MD5(t.conf_type) config_id
        , MD5(t.consumer_type) AS consumer_type_id
        , MD5(t.solar_zone) AS solar_zone_id
        , MD5(t.selfconsumption_status) AS selfconsumption_status_id
        , MD5(t.cnmc_solar) AS cnmc_solar_id
        , t.selfconsumption_id
        , t.cau_ID
        , t.compensation
        , t.connection_type
        , t.init_date
        , t.end_date
        , t.excedents
        , t.generation_pot
        , t.selfconsumption_owner    
    FROM base AS t
    LEFT JOIN LATERAL FLATTEN(
        input => t.cups,
        OUTER => TRUE
    ) AS f
)
SELECT * FROM silver_selfconsumptions