SELECT
    accounting_period,
    SUM(CASE WHEN account_code = 'DEFERRED_BOOKING_REVENUE' THEN reporting_credit_minor - reporting_debit_minor ELSE 0 END) AS net_deferred_revenue_usd_minor,
    SUM(CASE WHEN account_code = 'RECOGNIZED_PLATFORM_REVENUE' THEN reporting_credit_minor - reporting_debit_minor ELSE 0 END) AS recognized_platform_revenue_usd_minor,
    SUM(CASE WHEN account_code = 'REFUND_CONTRA_REVENUE' THEN reporting_debit_minor - reporting_credit_minor ELSE 0 END) AS refund_contra_revenue_usd_minor,
    SUM(CASE WHEN account_code = 'PAYMENT_PROCESSING_FEE' THEN reporting_debit_minor - reporting_credit_minor ELSE 0 END) AS psp_processing_fee_usd_minor,
    SUM(CASE WHEN account_code = 'CHARGEBACK_LOSS' THEN reporting_debit_minor - reporting_credit_minor ELSE 0 END) AS chargeback_loss_usd_minor,
    SUM(CASE WHEN account_code = 'FX_GAIN_LOSS' THEN reporting_credit_minor - reporting_debit_minor ELSE 0 END) AS net_fx_gain_loss_usd_minor
FROM {{ ref('stg_ledger_journal_entries') }}
GROUP BY accounting_period
ORDER BY accounting_period
