SELECT
    rate_card_id,
    partner_id,
    service_type,
    destination_country,
    destination_prefix,
    rate_per_unit,
    currency,
    effective_from,
    effective_to,
    rate_version,
    commitment_tier,
    peak_rate,
    offpeak_rate,
    created_at
FROM {{ source('raw_whl', 'raw_rate_cards') }}
