"""Automated pytest wrapper verifying all 7 financial chaos & fault-injection drills."""

from __future__ import annotations

import pytest

from scripts.run_chaos_drills import execute_all_chaos_drills


@pytest.mark.chaos
def test_all_seven_chaos_drills_pass() -> None:
    report = execute_all_chaos_drills()
    assert report["total_drills"] == 7
    assert report["failed_drills"] == 0, report
    assert report["all_passed"] is True
    for drill in report["drills"]:
        assert drill["passed"] is True, drill
