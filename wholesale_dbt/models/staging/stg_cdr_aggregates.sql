SELECT
    record_id,
    cdr_date,
    partner_id,
    service_type,
    traffic_direction,
    destination_country,
    destination_prefix,
    total_events,
    total_units,
    rated_amount,
    currency,
    rating_version,
    cdr_source,
    cdr_age_hours,
    reconciliation_status,
    created_at
FROM {{ source('raw_whl', 'raw_cdr_aggregates') }}
