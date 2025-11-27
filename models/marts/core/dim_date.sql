WITH date_gen AS (
    {{ dbt_date.get_date_dimension("2025-09-27", "2026-01-31") }}
),
date_cte AS (
    SELECT
        MD5(date_day) AS date_id,
        day_of_week,
        day_of_week_name,
        day_of_month,
        week_of_year,
        month_of_year,
        month_name
    FROM date_gen
)
SELECT *
FROM date_cte
