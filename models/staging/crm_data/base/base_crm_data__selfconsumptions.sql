WITH src_selfconsumptions AS (
    SELECT * 
    FROM {{ source('crm_data', 'selfconsumption') }}
    ),
renamed_casted AS (
    SELECT
        CAST(id AS VARCHAR) AS selfconsumption_id
        , CAST(cau_ID AS VARCHAR) AS cau_id
        , CAST(compensation AS BOOLEAN) compensation
        , COALESCE(NULLIF(TRIM(CAST(conf_type:conf_type AS VARCHAR)), ''), 'Unknown') AS configuration_type
        , COALESCE(NULLIF(TRIM(CAST(conf_type:conf_type_desciption AS VARCHAR)), ''), 'Unknown') AS conf_type_desc
        , CAST(conn_type AS VARCHAR) AS connection_type
        , MD5(CAST(consumer_type:consumer_type_id AS VARCHAR)) AS consumer_type_id
        , CAST(consumer_type:consumer_type_desc AS VARCHAR) AS consumer_type_desc
        , cups
        , CONVERT_TIMEZONE('UTC', end_date) AS end_date
        , CAST(excedents AS BOOLEAN) AS excedents
        , CAST(generation_pot AS FLOAT) AS generation_pot
        , MD5(name) AS selfconsumption_owner
        , MD5(CAST(solar_zone:solar_zone_id AS VARCHAR)) AS solar_zone_id
        , CAST(solar_zone:solar_zone_name AS VARCHAR) AS solar_zone_name
        , CAST(solar_zone:solar_zone_num AS INTEGER) AS solar_zone_number
        , MD5(CAST(status:status_id AS VARCHAR)) AS selfconsumption_status_id
        , CAST(status:status_name AS VARCHAR) AS status_name
        , MD5(CAST(unit_type:cnmc_type_id AS VARCHAR)) AS cnmc_type_id
        , CAST(unit_type:cnmc_type_desc AS VARCHAR) AS cnmc_type_desc
        , CAST(unit_type:cnmc_type_name AS INTEGER) AS cnmc_type_name
        , CONVERT_TIMEZONE('UTC', init_date) AS init_date
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_selfconsumptions
    )
SELECT * FROM renamed_casted