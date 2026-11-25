"""FastAPI Finance & Reconciliation Control Plane (port 8040).

Provides RBAC-protected endpoints for:
  - Trial balance inspection & zero-sum verification
  - Booking double-entry journal trace & bi-temporal as-of state lookup
  - 3-way reconciliation summary, exceptions, aging, and resolution workflow
  - Period close transitions (OPEN -> SOFT_CLOSE -> HARD_CLOSED) & late restatements
  - Financial Close Sign-Off Certificates, Iceberg V2 catalog metadata, DQ, OpenLineage, and Prometheus /metrics
  - Interactive Financial Close & Reconciliation Control Tower UI
"""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from booking_ledger.close_engine import (
    PeriodCloseError,
    close_accounting_period,
    post_late_restatement_entry,
)
from booking_ledger.config import REPO_ROOT, ROLE_RANK, Settings, load_settings
from booking_ledger.iceberg_store import IcebergLakehouseStore, query_duckdb_sql
from booking_ledger.ledger import compute_trial_balance, verify_ledger_zero_sum
from booking_ledger.lineage import load_lineage_events
from booking_ledger.logging_obs import METRICS, get_logger, redact_sensitive
from booking_ledger.money import FX_SCALE, format_minor
from booking_ledger.pipeline import run_full_pipeline
from booking_ledger.reconciliation import resolve_exception_ticket
from booking_ledger.temporal import query_booking_as_of

LOGGER = get_logger("finance_api")
DASHBOARD_DIR = REPO_ROOT / "apps" / "finance-dashboard"


class ResolveExceptionRequest(BaseModel):
    target_status: str = Field(default="RESOLVED_ADJUSTED", pattern="^(INVESTIGATING|RESOLVED_ADJUSTED)$")
    resolution_note: str = Field(min_length=3, max_length=400)
    open_period: str = Field(default="2026-05", pattern=r"^\d{4}-\d{2}$")


class ClosePeriodRequest(BaseModel):
    period: str = Field(pattern=r"^\d{4}-\d{2}$")
    target_state: str = Field(pattern="^(SOFT_CLOSE|HARD_CLOSED)$")
    inject_late_restatement_booking_id: str | None = None
    restatement_amount_minor: int | None = Field(default=None, gt=0)
    restatement_reason: str | None = None


class PipelineRunRequest(BaseModel):
    seed: int | None = None
    num_bookings: int | None = Field(default=None, ge=20, le=5000)


class FinanceRuntime:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.settings.ensure_dirs()
        self.store = IcebergLakehouseStore(settings)

    def ensure_initialized(self) -> dict[str, Any]:
        if not self.settings.state_snapshot_path.exists():
            return run_full_pipeline(self.settings)
        loaded: dict[str, Any] = json.loads(self.settings.state_snapshot_path.read_text(encoding="utf-8"))
        return loaded

    def record_audit(self, actor_role: str, action: str, target: str, details: dict[str, Any]) -> None:
        event = {
            "timestamp": datetime.now(UTC).isoformat(),
            "actor_role": actor_role,
            "action": action,
            "target": target,
            "details": redact_sensitive(details),
        }
        with self.settings.audit_log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event) + "\n")

    def read_audit_tail(self, limit: int = 25) -> list[dict[str, Any]]:
        if not self.settings.audit_log_path.exists():
            return []
        lines = [ln for ln in self.settings.audit_log_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        return [json.loads(ln) for ln in lines[-limit:]][::-1]


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or load_settings()
    runtime = FinanceRuntime(cfg)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        summary = runtime.ensure_initialized()
        # Hydrate Prometheus gauges from current Iceberg state on startup
        entries = runtime.store.read_table_rows("gold.ledger_journal_entries")
        exceptions = runtime.store.read_table_rows("gold.reconciliation_exceptions")
        closes = runtime.store.read_table_rows("gold.period_close_snapshots")
        dq_rows = runtime.store.read_table_rows("quality.dq_results")
        zero_sum = verify_ledger_zero_sum(entries)
        METRICS.trial_balance_imbalance_minor.set(zero_sum["global_imbalance_minor"])
        METRICS.reconciliation_match_rate.set(summary["reconciliation_summary"]["match_rate"])
        open_excs = [e for e in exceptions if e["status"] != "RESOLVED_ADJUSTED"]
        METRICS.open_exceptions_count.set(len(open_excs))
        METRICS.open_exception_exposure_usd_minor.set(sum(int(e["variance_usd_minor"]) for e in open_excs))
        dq_rate = (sum(1 for r in dq_rows if r["passed"]) / len(dq_rows)) if dq_rows else 1.0
        METRICS.dq_pass_rate.set(dq_rate)
        METRICS.close_periods_locked_total.set(sum(1 for c in closes if c["status"] == "HARD_CLOSED"))
        METRICS.mark_pipeline_success()
        yield

    app = FastAPI(
        title="Global Booking, Financial Ledger & Reconciliation Lakehouse API",
        version="0.1.0",
        description="Immutable double-entry accounting ledger, bi-temporal booking state, 3-way reconciliation, and period-close control plane.",
        lifespan=lifespan,
    )
    app.state.runtime = runtime

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next: Any) -> Response:
        t0 = time.perf_counter()
        response: Response = await call_next(request)
        elapsed = time.perf_counter() - t0
        path = request.url.path
        METRICS.api_requests_total.labels(
            method=request.method,
            path=path,
            status=str(response.status_code),
        ).inc()
        METRICS.api_request_duration_seconds.labels(path=path).observe(elapsed)
        return response

    def require_role(min_role: str):
        def _dep(x_api_key: str | None = Header(default=None, alias="X-API-Key")) -> str:
            role = cfg.role_for_api_key(x_api_key)
            if role is None:
                raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key")
            if ROLE_RANK[role] < ROLE_RANK[min_role]:
                raise HTTPException(
                    status_code=403,
                    detail=f"Role '{role}' lacks required privilege '{min_role}'",
                )
            return role

        return _dep

    @app.get("/health")
    def health() -> dict[str, Any]:
        snap = runtime.ensure_initialized()
        return {
            "status": "ok",
            "service": "global-booking-reconciliation-lakehouse",
            "runtime_mode": cfg.runtime_mode,
            "last_pipeline_run_id": snap.get("run_id"),
            "last_completed_at": snap.get("completed_at"),
            "trial_balance_balanced": snap.get("ledger_verification", {}).get("balanced", False),
        }

    @app.get("/metrics")
    def prometheus_metrics() -> Response:
        return Response(content=METRICS.render_prometheus(), media_type="text/plain; version=0.0.4")

    @app.get("/v1/overview")
    def overview(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        snap = runtime.ensure_initialized()
        entries = runtime.store.read_table_rows("gold.ledger_journal_entries")
        exceptions = runtime.store.read_table_rows("gold.reconciliation_exceptions")
        recon = runtime.store.read_table_rows("gold.reconciliation_results")
        closes = runtime.store.read_table_rows("gold.period_close_snapshots")
        dq_rows = runtime.store.read_table_rows("quality.dq_results")
        dlq_rows = runtime.store.read_table_rows("bronze.quarantine_dlq")
        tb = compute_trial_balance(entries)

        open_excs = [e for e in exceptions if e["status"] != "RESOLVED_ADJUSTED"]
        resolved_excs = [e for e in exceptions if e["status"] == "RESOLVED_ADJUSTED"]
        cls_counts: dict[str, int] = {}
        for row in recon:
            c = str(row["classification"])
            cls_counts[c] = cls_counts.get(c, 0) + 1
        clean_matched = cls_counts.get("MATCHED", 0) + cls_counts.get("DUPLICATE_FILE_DEDUPED", 0)
        match_rate = round(clean_matched / len(recon), 6) if recon else 1.0

        aging_buckets = {"0-1d": 0, "2-7d": 0, "8-30d": 0, ">30d": 0}
        for e in open_excs:
            b = str(e["aging_bucket"])
            if b in aging_buckets:
                aging_buckets[b] += 1

        # Revenue recognition summary from ledger
        recognized_rev = 0
        deferred_rev = 0
        refund_contra = 0
        chargeback_loss = 0
        for acc in tb["accounts"]:
            if acc["account_code"] == "RECOGNIZED_PLATFORM_REVENUE":
                recognized_rev = acc["reporting_credit_minor"] - acc["reporting_debit_minor"]
            elif acc["account_code"] == "DEFERRED_BOOKING_REVENUE":
                deferred_rev = acc["reporting_credit_minor"] - acc["reporting_debit_minor"]
            elif acc["account_code"] == "REFUND_CONTRA_REVENUE":
                refund_contra = acc["reporting_debit_minor"] - acc["reporting_credit_minor"]
            elif acc["account_code"] == "CHARGEBACK_LOSS":
                chargeback_loss = acc["reporting_debit_minor"] - acc["reporting_credit_minor"]

        return {
            "viewer_role": role,
            "freshness": {
                "run_id": snap.get("run_id"),
                "trace_id": snap.get("trace_id"),
                "completed_at": snap.get("completed_at"),
                "seed": snap.get("seed"),
                "num_bookings": snap.get("num_bookings"),
            },
            "kpis": {
                "total_reporting_debits_minor": tb["total_reporting_debits_minor"],
                "total_reporting_debits_fmt": format_minor(tb["total_reporting_debits_minor"], "USD"),
                "total_reporting_credits_minor": tb["total_reporting_credits_minor"],
                "total_reporting_credits_fmt": format_minor(tb["total_reporting_credits_minor"], "USD"),
                "trial_balance_imbalance_minor": tb["imbalance_minor"],
                "is_balanced": tb["is_balanced"],
                "ledger_entry_lines": len(entries),
                "restatement_lines": tb["restatement_line_count"],
                "recognized_revenue_usd_minor": recognized_rev,
                "recognized_revenue_usd_fmt": format_minor(recognized_rev, "USD"),
                "deferred_revenue_usd_minor": deferred_rev,
                "deferred_revenue_usd_fmt": format_minor(deferred_rev, "USD"),
                "refund_contra_usd_minor": refund_contra,
                "chargeback_loss_usd_minor": chargeback_loss,
                "reconciliation_total": len(recon),
                "reconciliation_match_rate": match_rate,
                "open_exception_count": len(open_excs),
                "resolved_exception_count": len(resolved_excs),
                "open_exception_exposure_usd_minor": sum(int(e["variance_usd_minor"]) for e in open_excs),
                "open_exception_exposure_usd_fmt": format_minor(
                    sum(int(e["variance_usd_minor"]) for e in open_excs), "USD"
                ),
                "quarantined_dlq_count": len(dlq_rows),
                "dq_checks_passed": sum(1 for r in dq_rows if r["passed"]),
                "dq_checks_total": len(dq_rows),
                "dq_pass_rate": round(sum(1 for r in dq_rows if r["passed"]) / len(dq_rows), 6) if dq_rows else 1.0,
            },
            "classification_breakdown": cls_counts,
            "aging_buckets": aging_buckets,
            "close_periods": [
                {
                    "accounting_period": c["accounting_period"],
                    "status": c["status"],
                    "closed_at": c["closed_at"],
                    "entry_line_count": c["entry_line_count"],
                    "restatement_line_count": c["restatement_line_count"],
                    "trial_balance_imbalance_minor": c["trial_balance_imbalance_minor"],
                    "certificate_sha256": c["certificate_sha256"],
                }
                for c in closes
            ],
            "injected_fault_counts": snap.get("injected_fault_counts", {}),
        }

    @app.get("/v1/ledger/trial-balance")
    def get_trial_balance(
        period: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}$"),
        role: str = Depends(require_role("reader")),
    ) -> dict[str, Any]:
        entries = runtime.store.read_table_rows("gold.ledger_journal_entries")
        tb = compute_trial_balance(entries, period=period)
        for acc in tb["accounts"]:
            acc["reporting_debit_fmt"] = format_minor(acc["reporting_debit_minor"], "USD")
            acc["reporting_credit_fmt"] = format_minor(acc["reporting_credit_minor"], "USD")
            acc["net_reporting_fmt"] = format_minor(acc["net_reporting_minor"], "USD")
        tb["viewer_role"] = role
        tb["total_reporting_debits_fmt"] = format_minor(tb["total_reporting_debits_minor"], "USD")
        tb["total_reporting_credits_fmt"] = format_minor(tb["total_reporting_credits_minor"], "USD")
        return tb

    @app.get("/v1/ledger/booking/{booking_id}")
    def get_booking_ledger_trace(
        booking_id: str,
        role: str = Depends(require_role("reader")),
    ) -> dict[str, Any]:
        entries = [
            e
            for e in runtime.store.read_table_rows("gold.ledger_journal_entries")
            if e["booking_id"] == booking_id
        ]
        history = [
            h
            for h in runtime.store.read_table_rows("silver.booking_state_history")
            if h["booking_id"] == booking_id
        ]
        recon = [
            r
            for r in runtime.store.read_table_rows("gold.reconciliation_results")
            if r["booking_id"] == booking_id
        ]
        if not entries and not history and not recon:
            raise HTTPException(status_code=404, detail=f"Booking '{booking_id}' not found")
        zero_sum = verify_ledger_zero_sum(entries) if entries else {"balanced": True, "global_imbalance_minor": 0}
        return {
            "viewer_role": role,
            "booking_id": booking_id,
            "state_history": sorted(history, key=lambda r: int(r["sequence_num"])),
            "journal_entries": entries,
            "ledger_balance_check": zero_sum,
            "reconciliation": recon[0] if recon else None,
        }

    @app.get("/v1/bookings/{booking_id}/as-of")
    def get_booking_as_of(
        booking_id: str,
        effective_at: str = Query(..., description="ISO-8601 UTC business timestamp"),
        recorded_at: str | None = Query(default=None, description="Optional ISO-8601 system time cutoff"),
        role: str = Depends(require_role("reader")),
    ) -> dict[str, Any]:
        history = runtime.store.read_table_rows("silver.booking_state_history")
        state = query_booking_as_of(history, booking_id, effective_at, recorded_at)
        if state is None:
            raise HTTPException(
                status_code=404,
                detail=f"No state found for booking '{booking_id}' as of effective_at={effective_at}",
            )
        return {"viewer_role": role, "as_of_effective_at": effective_at, "as_of_recorded_at": recorded_at, "state": state}

    @app.get("/v1/reconciliation/summary")
    def get_recon_summary(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        recon = runtime.store.read_table_rows("gold.reconciliation_results")
        exceptions = runtime.store.read_table_rows("gold.reconciliation_exceptions")
        cls_counts: dict[str, int] = {}
        cls_exposure: dict[str, int] = {}
        for row in recon:
            c = str(row["classification"])
            cls_counts[c] = cls_counts.get(c, 0) + 1
            cls_exposure[c] = cls_exposure.get(c, 0) + int(row["variance_usd_minor"])
        clean_matched = cls_counts.get("MATCHED", 0) + cls_counts.get("DUPLICATE_FILE_DEDUPED", 0)
        open_excs = [e for e in exceptions if e["status"] != "RESOLVED_ADJUSTED"]
        aging = {"0-1d": 0, "2-7d": 0, "8-30d": 0, ">30d": 0}
        for e in open_excs:
            aging[str(e["aging_bucket"])] = aging.get(str(e["aging_bucket"]), 0) + 1
        return {
            "viewer_role": role,
            "total_transactions": len(recon),
            "clean_matched": clean_matched,
            "match_rate": round(clean_matched / len(recon), 6) if recon else 1.0,
            "counts_by_classification": cls_counts,
            "exposure_usd_minor_by_classification": cls_exposure,
            "open_exception_count": len(open_excs),
            "resolved_exception_count": len(exceptions) - len(open_excs),
            "open_exception_exposure_usd_minor": sum(int(e["variance_usd_minor"]) for e in open_excs),
            "aging_buckets": aging,
        }

    @app.get("/v1/reconciliation/exceptions")
    def list_recon_exceptions(
        status: str | None = Query(default=None),
        classification: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
        role: str = Depends(require_role("reader")),
    ) -> dict[str, Any]:
        exceptions = runtime.store.read_table_rows("gold.reconciliation_exceptions")
        filtered = [
            e
            for e in exceptions
            if (status is None or e["status"] == status)
            and (classification is None or e["classification"] == classification)
        ]
        sev_rank = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        filtered.sort(
            key=lambda e: (
                sev_rank.get(str(e["severity"]), 3),
                -int(e["variance_usd_minor"]),
                str(e["exception_id"]),
            )
        )
        for e in filtered[:limit]:
            e["variance_usd_fmt"] = format_minor(int(e["variance_usd_minor"]), "USD")
        return {
            "viewer_role": role,
            "total_matching": len(filtered),
            "items": filtered[:limit],
        }

    @app.post("/v1/reconciliation/exceptions/{exception_id}/resolve")
    def resolve_exception(
        exception_id: str,
        payload: ResolveExceptionRequest,
        role: str = Depends(require_role("auditor")),
    ) -> dict[str, Any]:
        exceptions = runtime.store.read_table_rows("gold.reconciliation_exceptions")
        idx_found = next((i for i, e in enumerate(exceptions) if e["exception_id"] == exception_id), None)
        if idx_found is None:
            raise HTTPException(status_code=404, detail=f"Exception '{exception_id}' not found")
        updated_exc, adj_entries = resolve_exception_ticket(
            exceptions[idx_found],
            actor=f"api-{role}",
            resolution_note=payload.resolution_note,
            target_status=payload.target_status,
            open_period=payload.open_period,
        )
        exceptions[idx_found] = updated_exc
        runtime.store.overwrite_table("gold.reconciliation_exceptions", exceptions)
        if adj_entries:
            runtime.store.append_table("gold.ledger_journal_entries", adj_entries)
        runtime.store.sync_duckdb_warehouse()

        open_excs = [e for e in exceptions if e["status"] != "RESOLVED_ADJUSTED"]
        METRICS.open_exceptions_count.set(len(open_excs))
        METRICS.open_exception_exposure_usd_minor.set(sum(int(e["variance_usd_minor"]) for e in open_excs))
        runtime.record_audit(
            role,
            "RESOLVE_EXCEPTION",
            exception_id,
            {"target_status": payload.target_status, "adjustment_lines": len(adj_entries)},
        )
        return {
            "exception": updated_exc,
            "adjustment_journal_entries": adj_entries,
        }

    @app.get("/v1/close/periods")
    def list_close_periods(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        closes = runtime.store.read_table_rows("gold.period_close_snapshots")
        return {"viewer_role": role, "periods": closes}

    @app.post("/v1/close/period")
    def trigger_period_close(
        payload: ClosePeriodRequest,
        role: str = Depends(require_role("admin")),
    ) -> dict[str, Any]:
        closes = runtime.store.read_table_rows("gold.period_close_snapshots")
        entries = runtime.store.read_table_rows("gold.ledger_journal_entries")
        exceptions = runtime.store.read_table_rows("gold.reconciliation_exceptions")
        recon = runtime.store.read_table_rows("gold.reconciliation_results")
        dq_rows = runtime.store.read_table_rows("quality.dq_results")

        existing = {c["accounting_period"]: c for c in closes}
        current_state = existing.get(payload.period, {}).get("status", "OPEN")
        closed_periods = {p for p, c in existing.items() if c["status"] == "HARD_CLOSED"}

        restatement_lines: list[dict[str, Any]] = []
        if payload.inject_late_restatement_booking_id:
            if payload.period not in closed_periods:
                raise HTTPException(
                    status_code=409,
                    detail=f"Period {payload.period} must be HARD_CLOSED before posting a post-close restatement",
                )
            target_p, restatement_lines = post_late_restatement_entry(
                closed_period=payload.period,
                closed_periods=closed_periods,
                booking_id=payload.inject_late_restatement_booking_id,
                reason=payload.restatement_reason or "API-triggered post-close late adjustment",
                amount_minor=payload.restatement_amount_minor or 5_000,
                currency="USD",
                fx_rate_micros=FX_SCALE,
            )
            runtime.store.append_table("gold.ledger_journal_entries", restatement_lines)
            runtime.store.sync_duckdb_warehouse()
            runtime.record_audit(
                role,
                "POST_CLOSE_RESTATEMENT",
                payload.period,
                {"routed_to_period": target_p, "booking_id": payload.inject_late_restatement_booking_id},
            )
            return {
                "action": "POST_CLOSE_RESTATEMENT",
                "original_closed_period": payload.period,
                "routed_open_period": target_p,
                "restatement_entries": restatement_lines,
            }

        dq_rate = (sum(1 for r in dq_rows if r["passed"]) / len(dq_rows)) if dq_rows else 1.0
        clean_matched = sum(1 for r in recon if r["classification"] in ("MATCHED", "DUPLICATE_FILE_DEDUPED"))
        open_excs = [e for e in exceptions if e["status"] != "RESOLVED_ADJUSTED"]
        recon_summary = {
            "match_rate": round(clean_matched / len(recon), 6) if recon else 1.0,
            "open_exception_count": len(open_excs),
            "open_exception_exposure_usd_minor": sum(int(e["variance_usd_minor"]) for e in open_excs),
        }
        try:
            cert = close_accounting_period(
                period=payload.period,
                target_state=payload.target_state,
                current_state=current_state,
                ledger_entries=entries,
                recon_summary=recon_summary,
                dq_pass_rate=dq_rate,
                actor=f"api-{role}",
            )
        except PeriodCloseError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        existing[payload.period] = cert
        updated_closes = list(existing.values())
        runtime.store.overwrite_table("gold.period_close_snapshots", updated_closes)
        runtime.store.sync_duckdb_warehouse()
        METRICS.close_periods_locked_total.set(sum(1 for c in updated_closes if c["status"] == "HARD_CLOSED"))
        runtime.record_audit(role, "CLOSE_PERIOD", payload.period, {"target_state": payload.target_state})
        return {"certificate": cert}

    @app.get("/v1/quality/report")
    def quality_report(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        dq_rows = runtime.store.read_table_rows("quality.dq_results")
        dlq_rows = runtime.store.read_table_rows("bronze.quarantine_dlq")
        passed = sum(1 for r in dq_rows if r["passed"])
        return {
            "viewer_role": role,
            "summary": {
                "total_checks": len(dq_rows),
                "passed_checks": passed,
                "pass_rate": round(passed / len(dq_rows), 6) if dq_rows else 1.0,
                "quarantined_dlq_count": len(dlq_rows),
            },
            "checks": dq_rows,
            "dlq_sample": dlq_rows[:10],
        }

    @app.get("/v1/iceberg/tables")
    def iceberg_tables(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        return {
            "viewer_role": role,
            "catalog": "PyIceberg SQLite V2",
            "tables": runtime.store.list_table_metadata(),
        }

    @app.get("/v1/lineage")
    def lineage(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        events = load_lineage_events(cfg.lineage_dir)
        return {"viewer_role": role, "event_count": len(events), "recent_events": events[-5:]}

    @app.get("/v1/audit/events")
    def audit_events(role: str = Depends(require_role("auditor"))) -> dict[str, Any]:
        return {"viewer_role": role, "events": runtime.read_audit_tail(30)}

    @app.get("/v1/marts/revenue-recognition")
    def mart_revenue_recognition(role: str = Depends(require_role("reader"))) -> dict[str, Any]:
        if not cfg.duckdb_path.exists():
            return {"viewer_role": role, "rows": []}
        try:
            rows = query_duckdb_sql(
                cfg.duckdb_path,
                """
                SELECT accounting_period,
                       SUM(CASE WHEN account_code='DEFERRED_BOOKING_REVENUE' THEN reporting_credit_minor - reporting_debit_minor ELSE 0 END) AS deferred_usd_minor,
                       SUM(CASE WHEN account_code='RECOGNIZED_PLATFORM_REVENUE' THEN reporting_credit_minor - reporting_debit_minor ELSE 0 END) AS recognized_usd_minor,
                       SUM(CASE WHEN account_code='REFUND_CONTRA_REVENUE' THEN reporting_debit_minor - reporting_credit_minor ELSE 0 END) AS refund_contra_usd_minor,
                       SUM(CASE WHEN account_code='PAYMENT_PROCESSING_FEE' THEN reporting_debit_minor - reporting_credit_minor ELSE 0 END) AS psp_fee_usd_minor,
                       SUM(CASE WHEN account_code='CHARGEBACK_LOSS' THEN reporting_debit_minor - reporting_credit_minor ELSE 0 END) AS chargeback_usd_minor
                FROM gold.ledger_journal_entries
                GROUP BY accounting_period
                ORDER BY accounting_period
                """,
            )
            return {"viewer_role": role, "rows": rows}
        except Exception:
            return {"viewer_role": role, "rows": []}

    @app.post("/v1/pipeline/run")
    def trigger_pipeline_run(
        payload: PipelineRunRequest,
        role: str = Depends(require_role("admin")),
    ) -> dict[str, Any]:
        summary = run_full_pipeline(cfg, seed=payload.seed, num_bookings=payload.num_bookings)
        runtime.record_audit(role, "RUN_PIPELINE", summary["run_id"], {"bookings": summary["num_bookings"]})
        return summary

    if DASHBOARD_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")

        @app.get("/", include_in_schema=False)
        def serve_dashboard() -> FileResponse:
            return FileResponse(str(DASHBOARD_DIR / "index.html"))

    return app


app = create_app()
