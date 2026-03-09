SELECT
    dispute_id,
    partner_id,
    invoice_id,
    dispute_date,
    disputed_amount,
    dispute_reason,
    dispute_status,
    resolution_date,
    resolution_amount,
    created_at
FROM {{ source('raw_whl', 'raw_disputes') }}
