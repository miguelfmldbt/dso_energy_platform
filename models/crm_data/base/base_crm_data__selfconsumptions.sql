{{
    config(
        materialized='incremental',
        unique_key = 'selfconsumption_id',
        on_schema_change='sync'
    )
}}

WITH src_selfconsumptions AS (
    SELECT * 
    FROM {{ source('crm_data', 'selfconsumption') }}

    {% if is_incremental() %}

    where data_ingest > (select max(data_ingest) from {{ this }})

    {% endif %}

    ),
renamed_casted AS (
    SELECT
        CAST(id AS VARCHAR) AS selfconsumption_id
        , CAST(cau_ID AS VARCHAR) AS cau_id
        , CAST(compensation AS BOOLEAN) compensation
        , conf_type
        , CAST(conn_type AS VARCHAR) AS connection_type
        , consumer_type
        , cups
        , CONVERT_TIMEZONE('UTC', end_date) AS end_date
        , CAST(excedents AS BOOLEAN) AS excedents
        , CAST(generation_pot AS FLOAT) AS generation_pot
        , MD5(name) AS selfconsumption_owner
        , solar_zone
        , status AS selfconsumption_status
        , unit_type AS cnmc_solar
        , CONVERT_TIMEZONE('UTC', init_date) AS init_date
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_selfconsumptions
    )
SELECT * FROM renamed_casted