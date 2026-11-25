"""Structured JSON logging with PII redaction, OpenTelemetry tracing, and Prometheus metrics."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

TRACE_ID_CTX: ContextVar[str] = ContextVar("trace_id", default="")
RUN_ID_CTX: ContextVar[str] = ContextVar("run_id", default="")
CORRELATION_ID_CTX: ContextVar[str] = ContextVar("correlation_id", default="")

SENSITIVE_KEYS = {
    "api_key",
    "x-api-key",
    "authorization",
    "password",
    "secret",
    "token",
    "guest_email",
    "email",
    "card_number",
    "pan",
    "iban",
    "bank_account",
    "tax_id",
}
EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
CARD_PATTERN = re.compile(r"\b(?:\d[ -]*?){13,19}\b")


def redact_sensitive(obj: Any) -> Any:
    """Recursively redact PII and credential values before writing logs or audit trails."""
    if isinstance(obj, dict):
        redacted: dict[str, Any] = {}
        for k, v in obj.items():
            if str(k).lower() in SENSITIVE_KEYS:
                redacted[k] = "[REDACTED]"
            else:
                redacted[k] = redact_sensitive(v)
        return redacted
    if isinstance(obj, list):
        return [redact_sensitive(item) for item in obj]
    if isinstance(obj, str):
        masked = EMAIL_PATTERN.sub("[REDACTED_EMAIL]", obj)
        masked = CARD_PATTERN.sub("[REDACTED_PAN]", masked)
        return masked
    return obj


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_sensitive(record.getMessage()),
            "trace_id": TRACE_ID_CTX.get() or None,
            "run_id": RUN_ID_CTX.get() or None,
            "correlation_id": CORRELATION_ID_CTX.get() or None,
        }
        extra_fields = getattr(record, "structured", None)
        if isinstance(extra_fields, dict):
            payload["fields"] = redact_sensitive(extra_fields)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonLogFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


class JsonlSpanExporter(SpanExporter):
    """Exports completed OpenTelemetry spans to a local JSONL file for inspection."""

    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

    def export(self, spans: Any) -> SpanExportResult:
        try:
            with self.output_path.open("a", encoding="utf-8") as fh:
                for span in spans:
                    assert isinstance(span, ReadableSpan)
                    ctx = span.context
                    record = {
                        "name": span.name,
                        "trace_id": format(ctx.trace_id, "032x") if ctx else "",
                        "span_id": format(ctx.span_id, "016x") if ctx else "",
                        "start_time_ns": span.start_time,
                        "end_time_ns": span.end_time,
                        "duration_ms": round(((span.end_time or 0) - (span.start_time or 0)) / 1e6, 3),
                        "attributes": redact_sensitive(dict(span.attributes or {})),
                        "status": str(span.status.status_code.name),
                    }
                    fh.write(json.dumps(record) + "\n")
            return SpanExportResult.SUCCESS
        except Exception:
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        return None


_TRACER_CONFIGURED = False


def configure_tracing(traces_dir: Path) -> trace.Tracer:
    global _TRACER_CONFIGURED
    if not _TRACER_CONFIGURED:
        provider = TracerProvider(
            resource=Resource.create({"service.name": "global-booking-reconciliation-lakehouse"})
        )
        exporter = JsonlSpanExporter(traces_dir / "spans.jsonl")
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _TRACER_CONFIGURED = True
    return trace.get_tracer("booking_ledger")


@contextmanager
def traced_stage(
    stage_name: str,
    traces_dir: Path,
    attributes: dict[str, Any] | None = None,
) -> Iterator[str]:
    tracer = configure_tracing(traces_dir)
    corr_token = CORRELATION_ID_CTX.set(CORRELATION_ID_CTX.get() or uuid.uuid4().hex[:16])
    with tracer.start_as_current_span(stage_name) as span:
        span_ctx = span.get_span_context()
        trace_hex = format(span_ctx.trace_id, "032x") if span_ctx else uuid.uuid4().hex
        trace_token = TRACE_ID_CTX.set(trace_hex)
        if attributes:
            for key, value in redact_sensitive(attributes).items():
                if isinstance(value, (str, int, float, bool)):
                    span.set_attribute(key, value)
                else:
                    span.set_attribute(key, str(value))
        try:
            yield trace_hex
        finally:
            TRACE_ID_CTX.reset(trace_token)
            CORRELATION_ID_CTX.reset(corr_token)


class FinanceMetrics:
    """Dedicated Prometheus metrics registry for pipeline, ledger, reconciliation, and API."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.pipeline_runs_total = Counter(
            "finance_pipeline_runs_total",
            "Total medallion ledger & reconciliation pipeline runs",
            ["status"],
            registry=self.registry,
        )
        self.cdc_events_ingested_total = Counter(
            "finance_cdc_events_ingested_total",
            "Total CDC outbox events ingested into bronze Iceberg",
            ["event_type"],
            registry=self.registry,
        )
        self.dlq_events_total = Counter(
            "finance_dlq_events_total",
            "Total malformed or contract-violating events quarantined to DLQ",
            ["reason"],
            registry=self.registry,
        )
        self.ledger_entries_total = Counter(
            "finance_ledger_entries_total",
            "Total immutable double-entry accounting legs posted",
            ["entry_type"],
            registry=self.registry,
        )
        self.trial_balance_imbalance_minor = Gauge(
            "finance_trial_balance_imbalance_minor",
            "Absolute debit-minus-credit imbalance in reporting minor units (must be 0)",
            registry=self.registry,
        )
        self.reconciliation_match_rate = Gauge(
            "finance_reconciliation_match_rate",
            "Fraction of bookings cleanly matched across Ledger, PSP, and Bank",
            registry=self.registry,
        )
        self.open_exceptions_count = Gauge(
            "finance_open_exceptions_count",
            "Number of unresolved 3-way reconciliation exceptions",
            registry=self.registry,
        )
        self.open_exception_exposure_usd_minor = Gauge(
            "finance_open_exception_exposure_usd_minor",
            "Total USD minor-unit exposure across open reconciliation exceptions",
            registry=self.registry,
        )
        self.dq_pass_rate = Gauge(
            "finance_dq_pass_rate",
            "Data quality assertion pass rate (0.0 to 1.0)",
            registry=self.registry,
        )
        self.close_periods_locked_total = Gauge(
            "finance_close_periods_locked_total",
            "Number of HARD_CLOSED accounting periods",
            registry=self.registry,
        )
        self.api_requests_total = Counter(
            "finance_api_requests_total",
            "Total HTTP requests served by Finance Control Plane API",
            ["method", "path", "status"],
            registry=self.registry,
        )
        self.api_request_duration_seconds = Histogram(
            "finance_api_request_duration_seconds",
            "Finance API request latency in seconds",
            ["path"],
            registry=self.registry,
        )
        self.pipeline_last_success_timestamp = Gauge(
            "finance_pipeline_last_success_timestamp_seconds",
            "Unix timestamp of the latest successful pipeline run",
            registry=self.registry,
        )

    def render_prometheus(self) -> bytes:
        return generate_latest(self.registry)

    def mark_pipeline_success(self) -> None:
        self.pipeline_last_success_timestamp.set(time.time())


METRICS = FinanceMetrics()
