SELECT
    recon_id,
    booking_id,
    accounting_period,
    currency,
    ledger_net_minor,
    psp_net_minor,
    bank_settled_orig_minor,
    bank_settled_usd_minor,
    expected_bank_usd_minor,
    variance_minor,
    variance_usd_minor,
    classification,
    reason,
    booking_status,
    reconciled_at
FROM {{ source('gold', 'reconciliation_results') }}
