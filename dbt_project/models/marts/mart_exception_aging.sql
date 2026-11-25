SELECT
    aging_bucket,
    severity,
    status,
    COUNT(*) AS exception_count,
    SUM(variance_usd_minor) AS total_exposure_usd_minor
FROM {{ source('gold', 'reconciliation_exceptions') }}
GROUP BY aging_bucket, severity, status
ORDER BY aging_bucket, severity, status
