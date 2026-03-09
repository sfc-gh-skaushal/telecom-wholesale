{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

SELECT
    partner_id,
    partner_code,
    partner_name,
    partner_type,
    country_iso2,
    credit_limit_amt,
    payment_terms_days,
    active_flag
FROM {{ ref('stg_partners') }}
