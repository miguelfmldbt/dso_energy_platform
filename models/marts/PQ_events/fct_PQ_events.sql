{{
    config(
        materialized='incremental',
        unique_key = 'quality_event_id',
        on_schema_change='fail'
    )
}}

WITH stg_contracts AS (
    SELECT supplyPoint
    FROM {{ ref('stg_crm_data__contracts') }}
),

stg_supplypoints AS (
    SELECT
        supplyPoint,
        meter_supply_id
    FROM {{ ref('stg_crm_data__supply_points') }}
),

stg_meters AS (
    SELECT
        meter_supply_id,
        meter_id
    FROM {{ ref('stg_crm_data__meters') }}
),

master_supply_contracts_meters AS (
    SELECT 
        stg_contracts.supplyPoint,
        stg_meters.meter_id
    FROM stg_contracts
    LEFT JOIN stg_supplypoints
        ON stg_contracts.supplyPoint = stg_supplypoints.supplyPoint
    LEFT JOIN stg_meters
        ON stg_supplypoints.meter_supply_id = stg_meters.meter_supply_id    
),

fct_pq_events AS (
    SELECT
        meter_id,
        concentrator,
        MD5(CAST(quality_event_timestamp AS DATE)) AS date_id,
        quality_event_timestamp,
        CASE WHEN quality_event_group = 3 THEN 'PQ_event'
             ELSE 'Unknown_event_group' END AS quality_event_group_name,
        CASE
            WHEN quality_event_type IN ('13','14','15','16') THEN 'Undervoltage'
            WHEN quality_event_type IN ('17','18','19','20') THEN 'Overvoltage'
            WHEN quality_event_type IN ('21','22','23','24') THEN 'Power_Failure'
            ELSE 'Unknown event'
        END AS quality_event_type_desc,
        CASE 
            WHEN quality_event_start IS NULL THEN 0
            ELSE DATEDIFF('second', quality_event_start, quality_event_timestamp)
        END AS quality_event_duration_s
    FROM {{ ref('stg_s09_data__s09') }}
),

pq_events_grouped AS (
    SELECT
        MD5(meter_id || '|' || concentrator || '|' || DATE_TRUNC('HOUR', quality_event_timestamp)) 
            AS quality_event_id,
        meter_id,
        concentrator,
        date_id,
        DATE_TRUNC('HOUR', quality_event_timestamp) AS event_date_hour,
        DATE_PART('HOUR', DATE_TRUNC('HOUR', quality_event_timestamp)) AS event_hour,
        quality_event_group_name,
        quality_event_type_desc,
        COUNT(*) AS num_events,
        SUM(quality_event_duration_s) AS event_duration
    FROM fct_pq_events
    GROUP BY 
        meter_id,
        concentrator,
        date_id,
        DATE_TRUNC('HOUR', quality_event_timestamp),
        quality_event_group_name,
        quality_event_type_desc
),

-- El incremental se controla aquí, usando la clave FINAL
filtered_events AS (
    SELECT *
    FROM pq_events_grouped
    {% if is_incremental() %}
        WHERE quality_event_id NOT IN (SELECT quality_event_id FROM {{ this }})
    {% endif %}
)

SELECT
    f.quality_event_id,
    mscm.supplyPoint,
    f.meter_id,
    f.concentrator,
    f.date_id,
    f.event_date_hour,
    f.event_hour,
    f.quality_event_group_name,
    f.quality_event_type_desc,
    f.num_events,
    f.event_duration
FROM filtered_events f
LEFT JOIN master_supply_contracts_meters mscm
    ON f.meter_id = mscm.meter_id
WHERE mscm.supplyPoint IS NOT NULL