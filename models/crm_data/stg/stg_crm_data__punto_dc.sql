WITH src_punto_dc AS (
    SELECT * 
    FROM {{ source('crm_data', 'punto_dc') }}
    ),
silver_puntodc AS (
    SELECT
        CUPS AS supplyPoint
        , CAST(REPLACE("Coordenada X", ',', '.') AS FLOAT) AS x_coord
        , CAST(REPLACE("Coordenada Y", ',', '.') AS FLOAT) AS y_coord
        , CAST(REPLACE("Coordenada Z", ',', '.') AS FLOAT) AS z_coord
        , CONVERT_TIMEZONE('UTC', data_ingest) AS data_ingest
    FROM src_punto_dc
    )
SELECT * FROM silver_puntodc