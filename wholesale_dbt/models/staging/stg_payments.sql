SELECT
    payment_id,
    partner_id,
    invoice_id,
    payment_date,
    payment_amount,
    payment_currency,
    fx_rate_to_usd,
    payment_method,
    reference,
    is_partial,
    netting_offset_amount,
    created_at
FROM {{ source('raw_whl', 'raw_payments') }}
