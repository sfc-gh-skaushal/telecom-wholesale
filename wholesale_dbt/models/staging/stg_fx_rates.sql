SELECT
    rate_date,
    from_currency,
    to_currency,
    exchange_rate,
    created_at
FROM {{ source('raw_whl', 'raw_fx_rates') }}
