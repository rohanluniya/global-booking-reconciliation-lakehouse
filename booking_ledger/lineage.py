"""OpenLineage-compatible (Marquez v1 schema) run & dataset lineage emitter."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

OPENLINEAGE_SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
OPENLINEAGE_PRODUCER = "https://github.com/ankdeshm/global-booking-reconciliation-lakehouse/0.1.0"


def emit_openlineage_event(
    *,
    lineage_dir: Path,
    job_name: str,
    event_type: str,
    run_id: str | None = None,
    inputs: list[dict[str, Any]] | None = None,
    outputs: list[dict[str, Any]] | None = None,
    namespace: str = "booking-reconciliation-lakehouse",
) -> dict[str, Any]:
    """Build and persist an OpenLineage RunEvent JSON document for Marquez / audit inspection."""
    lineage_dir.mkdir(parents=True, exist_ok=True)
    resolved_run_id = run_id or str(uuid.uuid4())
    event = {
        "eventType": event_type,
        "eventTime": datetime.now(UTC).isoformat(),
        "run": {
            "runId": resolved_run_id,
            "facets": {},
        },
        "job": {
            "namespace": namespace,
            "name": job_name,
            "facets": {},
        },
        "inputs": [
            {
                "namespace": namespace,
                "name": item["name"],
                "facets": {
                    "dataQualityMetrics": {"rowCount": item.get("row_count", 0)},
                },
            }
            for item in (inputs or [])
        ],
        "outputs": [
            {
                "namespace": namespace,
                "name": item["name"],
                "facets": {
                    "outputStatistics": {"rowCount": item.get("row_count", 0)},
                },
            }
            for item in (outputs or [])
        ],
        "producer": OPENLINEAGE_PRODUCER,
        "schemaURL": OPENLINEAGE_SCHEMA_URL,
    }
    log_path = lineage_dir / "openlineage_events.jsonl"
    with log_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event) + "\n")
    return event


def load_lineage_events(lineage_dir: Path) -> list[dict[str, Any]]:
    log_path = lineage_dir / "openlineage_events.jsonl"
    if not log_path.exists():
        return []
    events: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if line.strip():
                events.append(json.loads(line))
    return events
