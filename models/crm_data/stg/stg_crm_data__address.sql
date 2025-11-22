WITH stg_address AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
clean AS (
    SELECT DISTINCT
    address
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:street AS VARCHAR))), ''), 'Unknown') AS street_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:town AS VARCHAR))), ''), 'Unknown') AS town_name
    , COALESCE(NULLIF(INITCAP(TRIM(CAST(address:postalCode AS VARCHAR))), ''), 'Unknown') AS postal_code
    , CAST(address:typeAddress AS VARCHAR) AS type_address
    , MD5(CAST(address:phoneOne AS VARCHAR)) AS phone
    , CAST(address:number AS VARCHAR) AS number
    , CAST(address:portal AS VARCHAR) AS portal
    , CAST(address:floor AS VARCHAR) AS floor
    , CAST(address:letter AS VARCHAR) AS letter
    , data_ingest
),
silver_address AS (
    SELECT
        MD5(address) AS address_id
        , MD5(UPPER(street_name || '|' || town_name || '|' || postal_code)) AS street_id
        , number
        , portal
        , floor
        , letter
        , postal_code
        , type_address
        , phone
        , data_ingest
    FROM clean
)

SELECT * FROM silver_address
