{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

SELECT
    invoice_id,
    invoice_number,
    partner_id,
    billing_period_start,
    billing_period_end,
    issue_date,
    due_date,
    invoice_currency,
    invoice_amount,
    tax_amount,
    status,
    payment_terms_days,
    created_at,
    updated_at
FROM {{ ref('stg_invoices') }}
