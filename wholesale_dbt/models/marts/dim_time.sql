{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

WITH date_spine AS (
    SELECT 
        DATEADD(day, seq4(), '2023-01-01'::DATE) as date_id
    FROM TABLE(GENERATOR(ROWCOUNT => 1100))
)
SELECT
    date_id,
    YEAR(date_id) as year,
    QUARTER(date_id) as quarter,
    MONTH(date_id) as month,
    WEEKOFYEAR(date_id) as week,
    DAY(date_id) as day_of_month
FROM date_spine
WHERE date_id <= CURRENT_DATE()
