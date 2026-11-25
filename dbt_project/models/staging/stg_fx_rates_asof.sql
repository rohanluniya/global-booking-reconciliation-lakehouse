SELECT
    fx_id,
    currency,
    reporting_currency,
    effective_date,
    effective_at,
    recorded_at,
    rate_version,
    rate_micros,
    source,
    is_revision,
    system_valid_from,
    system_valid_to,
    is_latest_version
FROM {{ source('silver', 'fx_rates_asof') }}
