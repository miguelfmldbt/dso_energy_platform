WITH src_supply_points AS (
    SELECT COUNT(DISTINCT CONCAT(cups,'|',distributorId)) AS cnt
    FROM {{ source('crm_data', 'supply_points') }}
),

src_contracts AS (
    SELECT COUNT(DISTINCT CONCAT(cups,'|',distributorId)) AS cnt
    FROM {{ source('crm_data', 'contracts') }}
)
SELECT *
FROM src_contracts AS c
CROSS JOIN src_supply_points AS sp
WHERE sp.cnt < c.cnt

