"""Configuration and RBAC settings for the Global Booking & Reconciliation Lakehouse."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Settings:
    runtime_mode: str = "local_native"
    data_dir: Path = REPO_ROOT / ".local_data"
    api_host: str = "127.0.0.1"
    api_port: int = 8040
    admin_api_key: str = "dev-finance-admin-key-change-me"
    auditor_api_key: str = "dev-finance-auditor-key-change-me"
    reader_api_key: str = "dev-finance-reader-key-change-me"
    default_seed: int = 20260505
    default_bookings: int = 600
    reporting_currency: str = "USD"

    @property
    def warehouse_dir(self) -> Path:
        return self.data_dir / "iceberg_warehouse"

    @property
    def catalog_db_path(self) -> Path:
        return self.data_dir / "iceberg_catalog.sqlite"

    @property
    def broker_dir(self) -> Path:
        return self.data_dir / "broker"

    @property
    def duckdb_path(self) -> Path:
        return self.data_dir / "lakehouse.duckdb"

    @property
    def lineage_dir(self) -> Path:
        return self.data_dir / "lineage"

    @property
    def traces_dir(self) -> Path:
        return self.data_dir / "traces"

    @property
    def audit_log_path(self) -> Path:
        return self.data_dir / "audit_events.jsonl"

    @property
    def state_snapshot_path(self) -> Path:
        return self.data_dir / "control_plane_state.json"

    def ensure_dirs(self) -> None:
        for path in (
            self.data_dir,
            self.warehouse_dir,
            self.broker_dir,
            self.lineage_dir,
            self.traces_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)

    def role_for_api_key(self, api_key: str | None) -> str | None:
        if not api_key:
            return None
        if api_key == self.admin_api_key:
            return "admin"
        if api_key == self.auditor_api_key:
            return "auditor"
        if api_key == self.reader_api_key:
            return "reader"
        return None


ROLE_RANK = {"reader": 1, "auditor": 2, "admin": 3}


def load_settings(data_dir: Path | None = None) -> Settings:
    raw_dir = data_dir or Path(os.getenv("FINANCE_DATA_DIR", str(REPO_ROOT / ".local_data")))
    if not raw_dir.is_absolute():
        raw_dir = (REPO_ROOT / raw_dir).resolve()
    return Settings(
        runtime_mode=os.getenv("FINANCE_RUNTIME_MODE", "local_native"),
        data_dir=raw_dir,
        api_host=os.getenv("FINANCE_API_HOST", "127.0.0.1"),
        api_port=int(os.getenv("FINANCE_API_PORT", "8040")),
        admin_api_key=os.getenv("FINANCE_ADMIN_API_KEY", "dev-finance-admin-key-change-me"),
        auditor_api_key=os.getenv("FINANCE_AUDITOR_API_KEY", "dev-finance-auditor-key-change-me"),
        reader_api_key=os.getenv("FINANCE_READER_API_KEY", "dev-finance-reader-key-change-me"),
        default_seed=int(os.getenv("FINANCE_DEFAULT_SEED", "20260505")),
        default_bookings=int(os.getenv("FINANCE_DEFAULT_BOOKINGS", "600")),
        reporting_currency=os.getenv("FINANCE_REPORTING_CURRENCY", "USD"),
    )
