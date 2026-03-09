{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

WITH invoice_usage AS (
    SELECT
        l.invoice_line_id,
        l.invoice_id,
        i.partner_id,
        l.service_type,
        l.usage_start_date,
        l.usage_end_date,
        SUM(c.rated_amount) as usage_rated_amount
    FROM {{ ref('stg_invoice_lines') }} l
    JOIN {{ ref('stg_invoices') }} i ON l.invoice_id = i.invoice_id
    LEFT JOIN {{ ref('stg_cdr_aggregates') }} c 
        ON c.partner_id = i.partner_id
        AND c.service_type = l.service_type
        AND c.cdr_date BETWEEN l.usage_start_date AND l.usage_end_date
    GROUP BY l.invoice_line_id, l.invoice_id, i.partner_id, l.service_type, 
             l.usage_start_date, l.usage_end_date
)

SELECT
    l.invoice_line_id,
    l.invoice_id,
    l.service_type,
    l.charge_category,
    l.rating_basis,
    l.usage_start_date,
    l.usage_end_date,
    l.billed_units,
    l.billed_amount,
    l.billed_currency,
    l.rate_applied,
    iu.usage_rated_amount as recomputed_amount,
    CASE
        WHEN iu.usage_rated_amount IS NULL THEN 'NO_USAGE_MATCH'
        WHEN ABS(l.billed_amount - iu.usage_rated_amount) < 0.01 THEN 'OK'
        WHEN l.billed_amount < iu.usage_rated_amount THEN 'UNDER_BILLED'
        WHEN l.billed_amount > iu.usage_rated_amount THEN 'OVER_BILLED'
        ELSE 'NO_USAGE_MATCH'
    END as validation_status,
    l.billed_amount - COALESCE(iu.usage_rated_amount, 0) as validation_delta
FROM {{ ref('stg_invoice_lines') }} l
LEFT JOIN invoice_usage iu ON l.invoice_line_id = iu.invoice_line_id
