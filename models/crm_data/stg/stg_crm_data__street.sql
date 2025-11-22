WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
clean AS (
    SELECT DISTINCT
    COALESCE(NULLIF(INITCAP(TRIM(CAST(address:street AS VARCHAR))), ''),'Unknown') AS street_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:postalCode AS VARCHAR))), ''),'Unknown') AS postal_code
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:town AS VARCHAR))), ''), 'Unknown') AS town_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:municipalityIneCode AS VARCHAR))), ''), 'Unknown') AS municipality_ine_code
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:municipality AS VARCHAR))), ''), 'Unknown') AS municipality_name
    , data_ingest
),
silver_street AS (
    SELECT
        MD5(UPPER(street_name || '|' || town_name || '|' || postal_code)) AS street_id
        , MD5(UPPER(town_name || '|' || municipality_name || '|' || municipality_ine_code)) AS town_id
        , data_ingest
    FROM clean
)
SELECT *
FROM silver_street

