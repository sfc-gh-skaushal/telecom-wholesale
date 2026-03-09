{{
    config(
        materialized='table',
        schema='product_whl'
    )
}}

WITH date_range AS (
    SELECT date_id FROM {{ ref('dim_time') }}
    WHERE date_id >= DATEADD(month, -6, CURRENT_DATE())
      AND date_id <= CURRENT_DATE()
),

ar_by_date AS (
    SELECT
        d.date_id,
        i.partner_id,
        i.invoice_id,
        i.due_date,
        i.invoice_amount,
        COALESCE(SUM(p.payment_amount), 0) as total_paid
    FROM date_range d
    CROSS JOIN {{ ref('fact_invoice_ar') }} i
    LEFT JOIN {{ ref('fact_payment_ar') }} p 
        ON p.invoice_id = i.invoice_id
        AND p.payment_date <= d.date_id
    WHERE i.issue_date <= d.date_id
      AND i.status NOT IN ('CANCELLED')
    GROUP BY d.date_id, i.partner_id, i.invoice_id, i.due_date, i.invoice_amount
),

ar_open AS (
    SELECT
        date_id,
        partner_id,
        invoice_id,
        due_date,
        invoice_amount - total_paid as open_amount
    FROM ar_by_date
    WHERE invoice_amount - total_paid > 0
)

SELECT
    a.date_id,
    a.partner_id,
    SUM(a.open_amount) as total_open_ar,
    SUM(CASE WHEN DATEDIFF(day, a.date_id, a.due_date) >= 0 THEN a.open_amount ELSE 0 END) as current_bucket,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) BETWEEN 1 AND 30 THEN a.open_amount ELSE 0 END) as bucket_1_30_days,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) BETWEEN 31 AND 60 THEN a.open_amount ELSE 0 END) as bucket_31_60_days,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) BETWEEN 61 AND 90 THEN a.open_amount ELSE 0 END) as bucket_61_90_days,
    SUM(CASE WHEN DATEDIFF(day, a.due_date, a.date_id) > 90 THEN a.open_amount ELSE 0 END) as bucket_90_plus_days,
    p.credit_limit_amt
FROM ar_open a
JOIN {{ ref('dim_partner') }} p ON p.partner_id = a.partner_id
GROUP BY a.date_id, a.partner_id, p.credit_limit_amt
