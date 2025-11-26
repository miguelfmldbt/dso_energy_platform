WITH max_ts AS (
    SELECT MAX(energy_timestamp) AS max_energy_ts
    FROM {{ ref('fct_energy_consumption') }}
),

fct_energy AS (
    SELECT
        e.meter_id,
        e.meter_code,
        e.concentrator,
        e.date_id,
        e.energy_timestamp,
        e.total_energy_Wh
    FROM {{ ref('fct_energy_consumption') }} e
    INNER JOIN max_ts
        ON TRUE
    WHERE e.energy_timestamp BETWEEN DATEADD(day, -7, max_energy_ts) AND max_energy_ts
),

dim_contracts_selfconsumption AS (
    SELECT *
    FROM {{ ref('dim_contracts_selfconsumption') }}
),

dim_date AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),

dim_electrical_info AS (
    SELECT
        meter_id,
        line_code,
        transformer_code,
        ct_name,
        dso_name
    FROM {{ ref('dim_electrical_info') }}
),

dim_address_info AS (
    SELECT
        meter_id,
        town_name,
        municipality_name,
        province_name
    FROM {{ ref('dim_address_info') }}
),

dim_supply_locations AS (
    SELECT
        meter_id,
        x_coord,
        y_coord,
        z_coord,
        latitude,
        longitude
    FROM {{ ref('dim_supply_locations') }}
),

mart_energy_consumption AS (
    SELECT
        COALESCE(dcs.supplyPoint, 'no_contract_supplyPoint') AS supplyPoint,
        COALESCE(dcs.tariff, 'no_contract_tariff') AS tariff,
        COALESCE(dcs.energy_provider_name, 'no_contract_provider') AS energy_provider_name,
        COALESCE(dcs.consumer_type_desc, 'no_selfconsumption') AS consumer_type_desc,
        COALESCE(dcs.generation_pot, 0) AS generation_pot,
        f.meter_code,
        f.concentrator,
        COALESCE(dei.line_code, 'Unknown') AS line_code,
        COALESCE(dei.transformer_code, 'Unknown') AS transformer_code,
        COALESCE(dei.ct_name, 'Unknown') AS ct_name,
        COALESCE(dei.dso_name, 'Unknown') AS dso_name,
        COALESCE(dai.town_name, 'Unknown') AS town_name,
        COALESCE(dai.municipality_name, 'Unknown') AS municipality_name,
        COALESCE(dai.province_name, 'Unknown') AS province_name,
        dsl.x_coord,
        dsl.y_coord,
        dsl.z_coord,
        dsl.latitude,
        dsl.longitude,
        f.energy_timestamp,
        f.total_energy_Wh,
        dd.day_of_week,
        dd.day_of_week_name,
        dd.day_of_month,
        dd.week_of_year,
        dd.month_of_year,
        dd.month_name
    FROM fct_energy AS f
    LEFT JOIN dim_contracts_selfconsumption AS dcs
        ON f.meter_id = dcs.meter_id
    LEFT JOIN dim_date dd
        ON f.date_id = dd.date_id
    LEFT JOIN dim_electrical_info AS dei
        ON f.meter_id = dei.meter_id
    LEFT JOIN dim_address_info AS dai
        ON f.meter_id = dai.meter_id
    LEFT JOIN dim_supply_locations AS dsl
        ON f.meter_id = dsl.meter_id       
)

SELECT *
FROM mart_energy_consumption
