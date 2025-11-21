WITH src_selfconsumption AS (
    SELECT * 
    FROM {{ source('crm_data', 'selfconsumption') }}
    ),
renamed_casted AS (
    SELECT
        id AS selfconsumption_id
        , cau_ID
        , compensation
        , conf_type
        , conn_type AS connection_type
        , consumer_type
        , cups
        , CONVERT_TIMEZONE('UTC', end_date) AS end_date
        , excedents
        , generation_pot
        , name AS selfconsumption_owner
        , solar_zone
        , status AS selfconsumption_status
        , unit_type AS cnmc_solar
        , CONVERT_TIMEZONE('UTC', init_date) AS init_date
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_selfconsumption
    )
SELECT * FROM renamed_casted