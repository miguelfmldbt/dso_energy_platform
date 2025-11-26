{{
    config(
        materialized='incremental',
        unique_key = 'quality_event_id',
        on_schema_change='fail'
    )
}}

WITH fct_pq_events AS (
    SELECT
    quality_event_id
    , meter_id
    , meter_code
    , concentrator
    , MD5(CAST(quality_event_timestamp AS DATE)) AS date_id
    , quality_event_timestamp
    , CASE
        WHEN quality_event_group = 3 THEN 'PQ_event'
        ELSE 'Unknown_event'
    END AS quality_event_group_name
    , CASE
        WHEN quality_event_type IN ('13', '14', '15', '16') THEN 'Undervoltage'
        WHEN quality_event_type IN ('17', '18', '19', '20') THEN 'Overvoltage'
        WHEN quality_event_type IN ('21', '22', '23', '24') THEN 'Power_Failure'
        ELSE 'Unknown event'
    END AS quality_event_type_desc
    , quality_event_start
    , CASE 
        WHEN quality_event_start IS NULL THEN 0
        ELSE DATEDIFF(
            'second',
            quality_event_start,
            quality_event_timestamp
        )
    END AS quality_event_duration_s
    FROM {{ ref('stg_s09_data__s09') }}

    {% if is_incremental() %}
    WHERE quality_event_id NOT IN (SELECT quality_event_id FROM {{ this }})
    {% endif %}
)
SELECT *
FROM fct_pq_events