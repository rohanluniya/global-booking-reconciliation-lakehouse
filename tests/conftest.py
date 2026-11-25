"""Shared pytest fixtures for the Global Booking & Reconciliation Lakehouse test suite."""

from __future__ import annotations

from pathlib import Path

import pytest

from booking_ledger.config import Settings, load_settings
from booking_ledger.simulator import SimulationBundle, generate_simulation_bundle


@pytest.fixture(scope="session")
def small_sim_bundle() -> SimulationBundle:
    return generate_simulation_bundle(seed=420505, num_bookings=90)


@pytest.fixture()
def isolated_settings(tmp_path: Path) -> Settings:
    return load_settings(data_dir=tmp_path / "lakehouse_data")
