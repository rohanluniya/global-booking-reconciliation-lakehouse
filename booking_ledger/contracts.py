"""Avro contract registry, FastAvro validation, schema evolution checks, and deterministic IDs."""

from __future__ import annotations

import hashlib
import io
import json
from functools import lru_cache
from typing import Any

import fastavro
from fastavro.schema import parse_schema
from fastavro.validation import ValidationError, validate

from booking_ledger.config import REPO_ROOT

SCHEMA_DIR = REPO_ROOT / "schemas" / "avro"


def deterministic_id(prefix: str, *parts: object, length: int = 16) -> str:
    """Create a stable, deterministic SHA-256 based identifier for idempotent replay."""
    joined = "|".join(str(p) for p in parts)
    digest = hashlib.sha256(joined.encode("utf-8")).hexdigest()[:length]
    return f"{prefix}_{digest}"


@lru_cache(maxsize=16)
def load_raw_schema(schema_filename: str) -> dict[str, Any]:
    path = SCHEMA_DIR / schema_filename
    if not path.exists():
        raise FileNotFoundError(f"Avro schema not found: {path}")
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


@lru_cache(maxsize=16)
def load_parsed_schema(schema_filename: str) -> Any:
    raw = load_raw_schema(schema_filename)
    return parse_schema(raw)


def validate_avro_record(record: dict[str, Any], schema_filename: str) -> tuple[bool, str | None]:
    """Validate a dictionary record against a pinned Avro schema."""
    schema = load_parsed_schema(schema_filename)
    try:
        validate(record, schema, raise_errors=True)
        return True, None
    except ValidationError as exc:
        return False, str(exc)
    except Exception as exc:
        return False, f"Unexpected Avro validation error: {exc}"


def verify_backward_compatibility(
    writer_schema_filename: str,
    reader_schema_filename: str,
    sample_record: dict[str, Any],
) -> dict[str, Any]:
    """Serialize with writer (v1) schema and deserialize with reader (v2) schema."""
    writer_schema = load_parsed_schema(writer_schema_filename)
    reader_schema = load_parsed_schema(reader_schema_filename)
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, writer_schema, sample_record)
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, writer_schema, reader_schema)
    if not isinstance(decoded, dict):
        raise TypeError(f"Expected dict record from Avro reader, got {type(decoded).__name__}")
    return dict(decoded)
