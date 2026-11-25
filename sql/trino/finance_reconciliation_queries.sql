-- Trino 484 Analytical Queries for Apache Iceberg V2 Booking & Financial Reconciliation Lakehouse
-- Compatible with Lakekeeper Iceberg REST catalog (`iceberg.gold.*`, `iceberg.silver.*`).

-- 1. Zero-Sum Trial Balance Verification per Accounting Period (exact BIGINT minor units)
SELECT
    accounting_period,
    account_code,
    account_category,
    COUNT(*) AS journal_lines,
    SUM(reporting_debit_minor) AS reporting_debits_usd_cents,
    SUM(reporting_credit_minor) AS reporting_credits_usd_cents,
    SUM(reporting_debit_minor) - SUM(reporting_credit_minor) AS net_balance_usd_cents
FROM iceberg.gold.ledger_journal_entries
GROUP BY accounting_period, account_code, account_category
ORDER BY accounting_period, account_code;

-- 2. Payout & Settlement 3-Way Reconciliation Bridge
SELECT
    accounting_period,
    classification,
    COUNT(*) AS bookings,
    SUM(ledger_net_minor) AS ledger_net_orig_minor,
    SUM(psp_net_minor) AS psp_net_orig_minor,
    SUM(bank_settled_usd_minor) AS bank_settled_usd_cents,
    SUM(variance_usd_minor) AS total_variance_usd_cents
FROM iceberg.gold.reconciliation_results
GROUP BY accounting_period, classification
ORDER BY accounting_period, bookings DESC;

-- 3. Bi-Temporal Booking State Reconstruction (As-Of Point-in-Time Query)
SELECT
    booking_id,
    sequence_num,
    booking_status,
    effective_at,
    recorded_at,
    host_timezone,
    host_local_date,
    utc_date,
    tz_cutoff_shifted,
    currency,
    gross_total_minor
FROM iceberg.silver.booking_state_history
WHERE effective_at <= '2026-04-30T23:59:59+00:00'
  AND (valid_to IS NULL OR valid_to > '2026-04-30T23:59:59+00:00')
ORDER BY booking_id;

-- 4. Iceberg V2 Time-Travel Snapshot Audit (compare period-close immutable snapshots)
SELECT
    committed_at,
    snapshot_id,
    operation,
    summary
FROM iceberg.gold."ledger_journal_entries$snapshots"
ORDER BY committed_at DESC
LIMIT 10;

-- 5. Open Exception Aging & Exposure Drilldown
SELECT
    aging_bucket,
    severity,
    classification,
    COUNT(*) AS open_tickets,
    SUM(variance_usd_minor) AS open_exposure_usd_cents
FROM iceberg.gold.reconciliation_exceptions
WHERE status <> 'RESOLVED_ADJUSTED'
GROUP BY aging_bucket, severity, classification
ORDER BY open_exposure_usd_cents DESC;
