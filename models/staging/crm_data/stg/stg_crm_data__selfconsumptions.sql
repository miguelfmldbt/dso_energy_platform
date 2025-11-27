{{
    config(
        materialized='incremental',
        unique_key = 'selfconsumption_id',
        on_schema_change='fail'
    )
}}

WITH base AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumptions') }}

    {% if is_incremental() %}
        WHERE selfconsumption_id NOT IN (
            SELECT DISTINCT selfconsumption_id 
            FROM {{ this }}
        )
    {% endif %}

    ),
silver_selfconsumptions AS (
    SELECT
        COALESCE(NULLIF(REPLACE(TO_VARCHAR(f.value), '"', ''), ''), 'Unknown') AS supplyPoint
        , MD5(t.configuration_type || '|' || t.conf_type_desc) AS config_id
        , consumer_type_id
        , solar_zone_id
        , selfconsumption_status_id
        , cnmc_type_id
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