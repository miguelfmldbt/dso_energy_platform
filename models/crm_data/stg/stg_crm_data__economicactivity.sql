WITH stg_contracts AS (
    SELECT * 
    FROM {{ ref('base_crm_data__contracts') }}
    ),
silver_econactivity AS (
    SELECT DISTINCT
        MD5(economicActivity) AS economicActivityID
        , CAST(economicActivity:cnae AS INTEGER) AS cnae
        , CAST(economicActivity:cnaeDescription AS VARCHAR) AS cnae_desc
        , CAST(economicActivity:group AS VARCHAR) AS cnae_group
        , data_ingest
    FROM stg_contracts
    )
SELECT * FROM silver_econactivity