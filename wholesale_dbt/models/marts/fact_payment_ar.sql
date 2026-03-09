{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

SELECT
    payment_id,
    partner_id,
    invoice_id,
    payment_date,
    payment_amount,
    payment_currency,
    fx_rate_to_usd as fx_rate_to_reporting,
    payment_amount * COALESCE(fx_rate_to_usd, 1) as amount_reporting_ccy,
    payment_method,
    reference
FROM {{ ref('stg_payments') }}
