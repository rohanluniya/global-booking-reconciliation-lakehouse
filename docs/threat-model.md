# Security Threat Model & Governance Controls

## Scope & Assets Protected

- **Financial Ledger Integrity (`gold.ledger_journal_entries`):** Unauthorized modification, silent deletion, or unbalanced injection could distort recognized revenue or host payouts.
- **Period Close Lock (`gold.period_close_snapshots`):** Premature close with failing DQ or retroactive alteration of a `HARD_CLOSED` period violates financial auditability.
- **Reconciliation Exceptions (`gold.reconciliation_exceptions`):** Unauthorized ticket dismissal could conceal missing treasury cash or uninvestigated chargebacks.
- **Guest/Host Metadata & API Credentials:** Email addresses, payment references, and API keys must never leak into logs, traces, or committed artifacts.

## STRIDE Threat Analysis & Implemented Controls

| Threat Category | Concrete Scenario | Implemented Control | Verification |
|---|---|---|---|
| **Spoofing** | Unauthenticated caller queries trial balance or triggers period close | Mandatory `X-API-Key` header on all `/v1/*` routes; returns `401 Unauthorized` on missing/invalid key | `tests/e2e/test_finance_api_e2e.py` |
| **Elevation of Privilege** | Read-only analyst attempts to resolve an exception or lock a month-end close | 3-tier RBAC (`reader < auditor < admin`): `reader` cannot POST resolutions (`403`), `auditor` cannot close periods (`403`) | `tests/e2e/test_finance_api_e2e.py` |
| **Tampering** | Malicious or buggy producer injects float monetary amounts or mutates closed-period rows | FastAvro contract gate quarantines non-integer money to `bronze.quarantine_dlq`; `assert_minor_int()` blocks floats; `HARD_CLOSED` periods reject reopening (`409`) and route late adjustments to next open period | `CHAOS-05`, `CHAOS-07`, `test_money_and_fx.py` |
| **Repudiation** | Operator resolves an exception without traceable evidence | Every mutating API call writes an immutable entry to `.local_data/audit_events.jsonl` (actor role, action, target, redacted details) and links adjustment `journal_entry_id` | `GET /v1/audit/events` |
| **Information Disclosure** | PII or secrets leak into application logs, OTel spans, or git repository | `redact_sensitive()` recursively scrubs emails, PANs, API keys, and tokens before writing JSON logs/spans/audit trails; `scripts/verify_static.py` checks repository hygiene | `test_finance_api_e2e.py`, `scripts/verify_static.py` |
| **Denial of Service / Supply Chain** | Poison CDC batch crashes pipeline or compromised/unpinned container image pulled | Schema-invalid poison events isolated to DLQ; all container tags and Python packages strictly pinned (`uv.lock`, zero rolling tags); CycloneDX SBOM and license audit automated | `scripts/check_licenses.py`, `scripts/generate_sbom.py` |

## Explicit Dev-vs-Production Security Limitations

1. **API Key Storage:** Local development defaults (`dev-finance-*-key-change-me` in `.env.example`) exist strictly for zero-friction local demos. Production deployments must replace static API keys with OIDC/JWT identity tokens backed by an enterprise IdP and short-lived secrets from a vault/KMS.
2. **Transport Security (TLS):** Local uvicorn binds exclusively to loopback (`127.0.0.1:8040`). Production deployments must terminate TLS 1.3 at an ingress/mesh proxy and enforce mTLS between Debezium, Kafka, Lakekeeper, and Spark.
3. **Regulatory Disclaimer:** While the ledger implements sound double-entry accounting invariants and restatement isolation, this repository is an engineering reference platform and is **not** certified for SOX, IFRS 15, ASC 606, or PCI-DSS compliance without external audit and organizational controls.
