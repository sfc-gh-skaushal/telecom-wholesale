SELECT
    partner_id,
    partner_code,
    partner_name,
    partner_type,
    country_iso2,
    credit_limit_amt,
    payment_terms_days,
    risk_rating,
    historical_dso_days,
    active_flag,
    netting_agreement_flag,
    created_at
FROM {{ source('raw_whl', 'raw_partners') }}
