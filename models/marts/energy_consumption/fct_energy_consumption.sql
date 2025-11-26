{{
    config(
        materialized='incremental',
        unique_key = 'energy_id',
        on_schema_change='fail'
    )
}}

WITH fct_energy AS (
    SELECT
    energy_id
    , meter_id
    , meter_code
    , concentrator
    , MD5(CAST(energy_timestamp AS DATE)) AS date_id
    , energy_timestamp
    , (active_imported_energy_Wh - active_exported_energy_Wh) AS total_energy_Wh
    FROM {{ ref('stg_s02_data__s02') }}

    {% if is_incremental() %}
    WHERE energy_id NOT IN (SELECT energy_id FROM {{ this }})
    {% endif %}
)
SELECT *
FROM fct_energy