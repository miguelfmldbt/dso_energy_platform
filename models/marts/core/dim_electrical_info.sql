{{ 
    config(
        materialized='table',
    )
}}

WITH stg_contracts AS (
    SELECT
        supplyPoint
    FROM {{ ref('stg_crm_data__contracts') }}
),

stg_supplypoints AS (
    SELECT
        supplyPoint,
        meter_supply_id,
        line_id
    FROM {{ ref('stg_crm_data__supply_points') }}
),

stg_meters AS (
    SELECT
        meter_supply_id,
        meter_id,
        meter_code
    FROM {{ ref('stg_crm_data__meters') }}
),

stg_lines AS (
    SELECT *
    FROM {{ ref('stg_crm_data__lines') }}
),

stg_transformers AS (
    SELECT *
    FROM {{ ref('stg_crm_data__transformers') }}
),

stg_centers AS (
    SELECT *
    FROM {{ ref('stg_crm_data__centers') }}
),

stg_distributors AS (
    SELECT *
    FROM {{ ref('stg_crm_data__distributors') }}
),

dim_electrical AS (
    SELECT
        MD5(stg_meters.meter_supply_id) as dim_electrical_id,
        stg_meters.meter_id,
        stg_meters.meter_code,
        stg_lines.line_code,
        stg_transformers.transformer_code,
        stg_centers.ct_name,
        stg_distributors.dso_name
    FROM stg_contracts
    LEFT JOIN stg_supplypoints
        ON stg_contracts.supplyPoint = stg_supplypoints.supplyPoint

    LEFT JOIN stg_meters
        ON stg_supplypoints.meter_supply_id = stg_meters.meter_supply_id 

    LEFT JOIN stg_lines
        ON stg_supplypoints.line_id = stg_lines.line_id
    LEFT JOIN stg_transformers

        ON stg_lines.transformer_id = stg_transformers.transformer_id
    LEFT JOIN stg_centers

        ON stg_transformers.ct_id = stg_centers.ct_id
    LEFT JOIN stg_distributors

        ON stg_centers.dso_id = stg_distributors.dso_id
)

SELECT *
FROM dim_electrical
