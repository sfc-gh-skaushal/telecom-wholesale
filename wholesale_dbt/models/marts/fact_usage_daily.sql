{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

SELECT
    cdr_date as usage_date,
    partner_id,
    service_type,
    traffic_direction as traffic_dir,
    destination_country as destination,
    SUM(total_events) as total_events,
    SUM(total_units) as total_units,
    SUM(rated_amount) as rated_revenue,
    currency
FROM {{ ref('stg_cdr_aggregates') }}
GROUP BY 
    cdr_date,
    partner_id,
    service_type,
    traffic_direction,
    destination_country,
    currency
