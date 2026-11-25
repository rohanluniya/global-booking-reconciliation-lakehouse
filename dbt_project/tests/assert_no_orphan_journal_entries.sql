-- Singular dbt test: fails if any individual journal_entry_id is unbalanced in original or reporting currency
SELECT
    journal_entry_id,
    SUM(debit_minor) AS orig_debit,
    SUM(credit_minor) AS orig_credit,
    SUM(reporting_debit_minor) AS rep_debit,
    SUM(reporting_credit_minor) AS rep_credit
FROM {{ ref('stg_ledger_journal_entries') }}
GROUP BY journal_entry_id
HAVING SUM(debit_minor) <> SUM(credit_minor)
    OR SUM(reporting_debit_minor) <> SUM(reporting_credit_minor)
