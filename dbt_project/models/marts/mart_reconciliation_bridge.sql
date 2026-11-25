SELECT
    accounting_period,
    classification,
    COUNT(*) AS transaction_count,
    SUM(ledger_net_minor) AS sum_ledger_net_orig_minor,
    SUM(psp_net_minor) AS sum_psp_net_orig_minor,
    SUM(bank_settled_usd_minor) AS sum_bank_settled_usd_minor,
    SUM(expected_bank_usd_minor) AS sum_expected_bank_usd_minor,
    SUM(variance_usd_minor) AS total_variance_usd_minor
FROM {{ ref('stg_reconciliation_results') }}
GROUP BY accounting_period, classification
ORDER BY accounting_period, transaction_count DESC
