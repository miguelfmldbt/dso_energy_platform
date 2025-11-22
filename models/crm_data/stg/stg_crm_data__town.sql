WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
clean AS (
    SELECT DISTINCT
    COALESCE(NULLIF(INITCAP(TRIM(CAST(address:municipality AS VARCHAR))), ''), 'Unknown') AS municipality_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:municipalityIneCode AS VARCHAR))), ''), 'Unknown') AS municipality_ine_code
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:town AS VARCHAR))), ''), 'Unknown') AS town_name
    , data_ingest
    FROM base
),  
silver_town AS (
    SELECT
    MD5(UPPER(town_name || '|' || municipality_name || '|' || municipality_ine_code)) AS town_id
    , MD5(UPPER(municipality_name || '|' || municipality_ine_code)) AS municipality_id
    , data_ingest
    FROM clean
)
SELECT * FROM silver_town
