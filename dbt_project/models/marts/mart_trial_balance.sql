SELECT
    accounting_period,
    account_code,
    account_category,
    COUNT(*) AS line_count,
    SUM(reporting_debit_minor) AS total_reporting_debit_minor,
    SUM(reporting_credit_minor) AS total_reporting_credit_minor,
    SUM(reporting_debit_minor) - SUM(reporting_credit_minor) AS net_reporting_minor,
    SUM(CASE WHEN is_restatement THEN 1 ELSE 0 END) AS restatement_lines
FROM {{ ref('stg_ledger_journal_entries') }}
GROUP BY accounting_period, account_code, account_category
ORDER BY accounting_period, account_code
