-- Singular dbt test: fails if any accounting period has non-zero debit-credit imbalance
SELECT
    accounting_period,
    SUM(total_reporting_debit_minor) AS sum_debit,
    SUM(total_reporting_credit_minor) AS sum_credit,
    SUM(total_reporting_debit_minor) - SUM(total_reporting_credit_minor) AS imbalance
FROM {{ ref('mart_trial_balance') }}
GROUP BY accounting_period
HAVING SUM(total_reporting_debit_minor) <> SUM(total_reporting_credit_minor)
