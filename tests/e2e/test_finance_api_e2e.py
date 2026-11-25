"""End-to-end tests for FastAPI Finance Control Plane: RBAC, trial balance, booking trace, exception resolution, close, metrics, PII redaction."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from booking_ledger.config import Settings
from booking_ledger.logging_obs import redact_sensitive
from booking_ledger.pipeline import run_full_pipeline
from services.finance_api.main import create_app


@pytest.mark.e2e
def test_finance_api_rbac_and_full_workflows(isolated_settings: Settings) -> None:
    run_full_pipeline(isolated_settings, seed=8805, num_bookings=75)
    app = create_app(isolated_settings)

    with TestClient(app) as client:
        # 1. Public health & Prometheus metrics
        h = client.get("/health")
        assert h.status_code == 200
        assert h.json()["trial_balance_balanced"] is True

        m = client.get("/metrics")
        assert m.status_code == 200
        assert "finance_trial_balance_imbalance_minor 0.0" in m.text

        # 2. Access control rejects missing/bad API keys and enforces least privilege
        assert client.get("/v1/overview").status_code == 401
        assert client.get("/v1/overview", headers={"X-API-Key": "wrong-key"}).status_code == 401

        reader_hdr = {"X-API-Key": isolated_settings.reader_api_key}
        auditor_hdr = {"X-API-Key": isolated_settings.auditor_api_key}
        admin_hdr = {"X-API-Key": isolated_settings.admin_api_key}

        ov = client.get("/v1/overview", headers=reader_hdr)
        assert ov.status_code == 200
        assert ov.json()["kpis"]["trial_balance_imbalance_minor"] == 0

        # Reader cannot resolve exceptions or close periods (403 Forbidden)
        excs = client.get("/v1/reconciliation/exceptions?status=OPEN", headers=reader_hdr).json()["items"]
        assert len(excs) > 0
        exc_id = excs[0]["exception_id"]
        assert (
            client.post(
                f"/v1/reconciliation/exceptions/{exc_id}/resolve",
                headers=reader_hdr,
                json={"target_status": "RESOLVED_ADJUSTED", "resolution_note": "Blocked reader test"},
            ).status_code
            == 403
        )

        # Auditor CAN resolve exceptions and posts a balanced adjustment JE
        res_exc = client.post(
            f"/v1/reconciliation/exceptions/{exc_id}/resolve",
            headers=auditor_hdr,
            json={"target_status": "RESOLVED_ADJUSTED", "resolution_note": "Auditor approved adjustment"},
        )
        assert res_exc.status_code == 200
        assert res_exc.json()["exception"]["status"] == "RESOLVED_ADJUSTED"

        # Auditor CANNOT close period (requires admin -> 403)
        assert (
            client.post(
                "/v1/close/period",
                headers=auditor_hdr,
                json={"period": "2026-05", "target_state": "HARD_CLOSED"},
            ).status_code
            == 403
        )

        # Admin posts post-close restatement against HARD_CLOSED 2026-04 -> routed to 2026-05
        rst = client.post(
            "/v1/close/period",
            headers=admin_hdr,
            json={
                "period": "2026-04",
                "target_state": "HARD_CLOSED",
                "inject_late_restatement_booking_id": "BKG-2026-00001",
                "restatement_amount_minor": 4_200,
                "restatement_reason": "E2E post-close adjustment",
            },
        )
        assert rst.status_code == 200
        assert rst.json()["routed_open_period"] == "2026-05"

        # Trial balance remains strictly zero-sum after resolution + restatement
        tb = client.get("/v1/ledger/trial-balance", headers=reader_hdr).json()
        assert tb["is_balanced"] is True
        assert tb["imbalance_minor"] == 0

        # Booking trace & bi-temporal as-of endpoints
        trace = client.get("/v1/ledger/booking/BKG-2026-00001", headers=reader_hdr)
        assert trace.status_code == 200
        assert trace.json()["ledger_balance_check"]["balanced"] is True
        first_eff = trace.json()["state_history"][0]["effective_at"]
        as_of = client.get(
            "/v1/bookings/BKG-2026-00001/as-of",
            params={"effective_at": first_eff},
            headers=reader_hdr,
        )
        assert as_of.status_code == 200

        # Dashboard HTML served at /
        dash = client.get("/")
        assert dash.status_code == 200
        assert "Financial Reconciliation Control Tower" in dash.text

        # Audit log populated and redacts sensitive fields
        audit = client.get("/v1/audit/events", headers=auditor_hdr).json()["events"]
        assert len(audit) >= 2
        redacted = redact_sensitive({"guest_email": "alice@example.com", "x-api-key": "secret123"})
        assert redacted["guest_email"] == "[REDACTED]"
        assert redacted["x-api-key"] == "[REDACTED]"
