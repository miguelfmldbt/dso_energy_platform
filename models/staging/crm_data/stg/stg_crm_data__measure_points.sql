{{
    config(
        materialized='table'
    )
}}

WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_measure_point AS (
    SELECT DISTINCT
        MD5(measure_tension_level || '|' || tension_section) AS measure_point_id
        , measure_tension_level
        , tension_section
    FROM base
)
SELECT *
FROM silver_measure_point