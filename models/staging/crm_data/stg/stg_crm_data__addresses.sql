WITH base AS (
    SELECT *
    FROM {{ ref('base_crm_data__supply_points') }}
),
silver_address AS (
    SELECT DISTINCT
        MD5(
            street_name || '|' ||
            postal_code || '|' ||
            town_name || '|' ||
            municipality_name || '|' ||
            municipality_ine_code || '|' ||
            province_name || '|' ||
            province_ine_code || '|' ||
            number || '|' ||
            portal || '|' ||
            floor || '|' ||
            letter || '|' ||
            type_address || '|' ||
            phone
            ) AS address_id,
        MD5(street_name || '|' || postal_code || '|' || town_name || '|' || municipality_name || '|' || municipality_ine_code || '|' || province_name || '|' || province_ine_code) AS street_id,
        number,
        portal,
        floor,
        letter,
        type_address,
        phone
    FROM base
)

SELECT * FROM silver_address
