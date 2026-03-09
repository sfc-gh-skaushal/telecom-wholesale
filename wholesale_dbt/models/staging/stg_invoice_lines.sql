SELECT
    invoice_line_id,
    invoice_id,
    service_type,
    charge_category,
    rating_basis,
    usage_start_date,
    usage_end_date,
    billed_units,
    billed_amount,
    billed_currency,
    rate_applied,
    rate_plan_version,
    created_at
FROM {{ source('raw_whl', 'raw_invoice_lines') }}
