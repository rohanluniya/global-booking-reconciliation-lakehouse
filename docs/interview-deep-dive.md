# Staff/Principal Data Engineering Interview Deep Dive

## High-Signal Architecture Questions & Concrete Tradeoffs

### 1. Why enforce exact integer minor units instead of SQL `NUMERIC`/`DECIMAL` or IEEE-754 `DOUBLE`?

- **Tradeoff:** Storing monetary fields as `BIGINT` minor units (`room_subtotal_minor`, `reporting_debit_minor`) paired with explicit ISO-4217 currency precision (`CURRENCY_DECIMALS`) guarantees exact integer summation in Arrow, Parquet, Iceberg, DuckDB, Trino, and Spark with zero decimal-scale coercion surprises across engines.
- **Evidence:** On our 50,000-leg multi-currency FX audit (`benchmarks/run_benchmarks.py`), naive IEEE-754 `float64` accumulated **24,809 per-transaction mismatches** and **$0.71** in silent drift, whereas integer minor units + `Decimal(ROUND_HALF_EVEN)` produced **0 cents** drift.
- **Caveat:** Every display/API layer must consult currency decimals (`JPY = 0`, `USD/EUR/GBP/CAD/AUD = 2`); raw minor units must never be divided blindly by `100`.

### 2. How do you guarantee zero-sum reporting currency trial balances on multi-leg foreign currency journal entries?

- **Problem:** When a 4-leg EUR booking capture (`gross = room_net + deferred_rev + tax`) is translated leg-by-leg into USD cents using an FX rate like `1.084375`, rounding each leg independently can cause `SUM(reporting_debit) - SUM(reporting_credit)` to differ by `±1` cent.
- **Solution (`booking_ledger/ledger.py`):** Rather than silently fudging one revenue or payable leg, `build_balanced_journal_entry()` explicitly books any non-zero translation rounding residual (`±1` minor unit) to `FX_GAIN_LOSS` inside the same `journal_entry_id`. Both original-currency legs and reporting-currency legs balance identically to zero.

### 3. How does the platform distinguish at-least-once CDC transport from exactly-once accounting effects?

- **Transport:** Debezium + Kafka (or `LocalEventBroker`) guarantees **at-least-once** delivery from PostgreSQL `booking_core.outbox_events` (written inside the same ACID transaction as booking/payment updates).
- **Idempotent Effect:** Downstream consumers deduplicate by deterministic SHA-256 `event_id` and compute deterministic `journal_entry_id` / `entry_line_id` keys. Replaying the entire CDC topic twice (`CHAOS-01`) or reversing its arrival order (`CHAOS-02`) yields the exact same trial balance to the single cent.

### 4. Why model `booking_state_history` and `fx_rates_asof` bi-temporally instead of standard SCD Type 2?

- **Distinction:** Standard SCD Type 2 tracks only one timeline (`valid_from` / `valid_to`). Financial close requires **two** timelines: business time (`effective_at` — when the stay or FX fix occurred) and system time (`recorded_at` — when finance received the event or FX correction).
- **Concrete Example:** When a central bank publishes `v2_revised` FX two days late (`CHAOS-04`), querying `lookup_fx_as_of(..., recorded_as_of=capture.recorded_at)` reproduces the exact `v1` rate originally used at capture, while comparing against latest `v2_revised` generates a separate, balanced `FX_REVISION_ADJUSTMENT` entry without mutating history.

### 5. How do you handle a chargeback arriving 10 days after April month-end close?

- **Rule:** Once `2026-04` transitions to `HARD_CLOSED` (gated on `trial_balance_imbalance_minor == 0` and `dq_pass_rate == 1.0`), its Iceberg snapshot and SHA-256 Sign-Off Certificate are immutable.
- **Mechanism (`booking_ledger/close_engine.py`):** `post_late_restatement_entry()` rejects mutating `2026-04` and emits balanced legs into the next open period (`2026-05`) tagged with `is_restatement = true` and `original_period = "2026-04"`. April's published trial balance remains unchanged (`CHAOS-05`).
