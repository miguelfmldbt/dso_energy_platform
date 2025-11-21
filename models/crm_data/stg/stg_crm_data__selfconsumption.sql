WITH stg_selfconsumption AS (
    SELECT * 
    FROM {{ ref('base_crm_data__selfconsumption') }}
    ),
silver_selfconsumption AS (
    SELECT
         REPLACE(TO_VARCHAR(f.value), '"', '') AS cups
        ,t.selfconsumption_id
        , t.cau_ID
        , t.compensation
        , MD5(t.conf_type) config_id
        , t.connection_type
        , MD5(t.consumer_type) AS consumer_type_id
        , t.end_date
        , t.excedents
        , t.generation_pot
        , t.selfconsumption_owner
        , MD5(t.solar_zone) AS solar_zone_id
        , MD5(t.selfconsumption_status) AS selfconsumption_status_id
        , MD5(t.cnmc_solar) AS cnmc_solar_id
        , t.init_date
        , t.data_ingest
    FROM stg_selfconsumption AS t
    LEFT JOIN LATERAL FLATTEN(
        input => t.cups,
        OUTER => TRUE
    ) AS f;
)
SELECT * FROM silver_selfconsumption