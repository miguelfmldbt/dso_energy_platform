WITH src_supplypoints AS (
    SELECT * 
    FROM {{ source('crm_data', 'supply_points') }}
    WHERE serviceType = 'D-C' --Filter regular supply_points

    ),
renamed_casted AS (
    SELECT
        cups AS supplyPoint,
        COALESCE(NULLIF(TRIM(CAST(address:street AS VARCHAR)), ''), 'Unknown') AS street_name,
        COALESCE(
            NULLIF(
                TRIM(
                    REGEXP_REPLACE(
                        INITCAP(LOWER(CAST(address:town AS VARCHAR))),
                        '\\((.)(.*?)\\)',
                        '(' || UPPER('\\1') || '\\2)'
                    )
                ),
                ''
            ),
            'Unknown'
        ) AS town_name,
        COALESCE(NULLIF(TRIM(CAST(address:postalCode AS VARCHAR)), ''), 'Unknown')        AS postal_code,
        COALESCE(NULLIF(TRIM(CAST(address:typeAddress AS VARCHAR)), ''), 'Unknown')       AS type_address,
        COALESCE(NULLIF(TRIM(CAST(address:phoneOne AS VARCHAR)), ''), 'Unknown')          AS phone,
        COALESCE(NULLIF(TRIM(CAST(address:number AS VARCHAR)), ''), 'Unknown')            AS number,
        COALESCE(NULLIF(TRIM(CAST(address:portal AS VARCHAR)), ''), 'Unknown')            AS portal,
        COALESCE(NULLIF(TRIM(CAST(address:floor AS VARCHAR)), ''), 'Unknown')             AS floor,
        COALESCE(NULLIF(TRIM(CAST(address:letter AS VARCHAR)), ''), 'Unknown')            AS letter,
        COALESCE(NULLIF(TRIM(CAST(address:province AS VARCHAR)), ''), 'Unknown')          AS province_name,
        COALESCE(NULLIF(TRIM(CAST(address:provinceIneCode AS VARCHAR)), ''), 'Unknown')   AS province_ine_code,
        COALESCE(
            NULLIF(
                TRIM(
                    REGEXP_REPLACE(
                        INITCAP(LOWER(CAST(address:municipality AS VARCHAR))),
                        '\\((.)(.*?)\\)',
                        '(' || UPPER('\\1') || '\\2)'
                    )
                ),
                ''
            ),
            'Unknown'
        ) AS municipality_name,
        COALESCE(NULLIF(TRIM(CAST(address:municipalityIneCode AS VARCHAR)), ''), 'Unknown') AS municipality_ine_code,

        COALESCE(NULLIF(TRIM(ctCode), ''), 'Unknown') AS ct_code,
        COALESCE(NULLIF(TRIM(ctName), ''), 'Unknown') AS ct_name,
        COALESCE(NULLIF(TRIM(trafoCode), ''), 'Unknown') AS transformer_code,
        COALESCE(
            NULLIF(
                IFF(REGEXP_LIKE(lineCode, '^[0-9]+$'), 
                    LPAD(TO_NUMBER(lineCode), 1, '0'),
                    lineCode),
                ''),
            'Unknown') AS line_code,

        COALESCE(NULLIF(CAST(distributorId AS VARCHAR), ''), 'Unknown') AS dso_id,
        COALESCE(NULLIF(CAST(distributorCdos AS VARCHAR), ''), 'Unknown') AS dso_name,
        COALESCE(NULLIF(CAST(measureTensionLevel AS VARCHAR), ''), 'Unknown') AS measure_tension_level,
        CAST(tensionSection AS FLOAT) AS tension_section,
        COALESCE(NULLIF(meterSerialNumber, ''), 'Unknown') AS meter_code,
        COALESCE(NULLIF(CAST(meterTechCode AS VARCHAR), ''), 'Unknown') AS meter_tech_code,
        COALESCE(NULLIF(CAST(meterTechName AS VARCHAR), ''), 'Unknown') AS meter_tech_name,
        COALESCE(NULLIF(phase, ''), 'Unknown') AS phase,
        COALESCE(NULLIF(pointType, ''), 'Unknown') AS point_type,
        
        CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_supplypoints
    )
SELECT * FROM renamed_casted