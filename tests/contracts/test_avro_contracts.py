"""Contract tests for Avro v1/v2 schemas, backward compatibility, and poison rejection."""

from __future__ import annotations

import pytest

from booking_ledger.contracts import validate_avro_record, verify_backward_compatibility
from booking_ledger.simulator import SimulationBundle


@pytest.mark.contract
def test_avro_v1_validates_good_events_and_rejects_poison(small_sim_bundle: SimulationBundle) -> None:
    valid_count = 0
    invalid_count = 0
    for ev in small_sim_bundle.cdc_events:
        ok, _ = validate_avro_record(ev, "booking_cdc_event.v1.avsc")
        if ok:
            valid_count += 1
        else:
            invalid_count += 1
    assert valid_count > 100
    assert invalid_count == small_sim_bundle.injected_fault_counts["poison_schema_events"]


@pytest.mark.contract
def test_avro_v1_to_v2_backward_compatibility(small_sim_bundle: SimulationBundle) -> None:
    sample = next(e for e in small_sim_bundle.cdc_events if validate_avro_record(e, "booking_cdc_event.v1.avsc")[0])
    evolved = verify_backward_compatibility(
        "booking_cdc_event.v1.avsc",
        "booking_cdc_event.v2.avsc",
        sample,
    )
    assert evolved["booking_id"] == sample["booking_id"]
    assert evolved["gross_total_minor"] == sample["gross_total_minor"]
    assert evolved["schema_version"] == 2
    assert evolved["correlation_id"] is None
    assert evolved["restatement_reason"] is None


@pytest.mark.contract
def test_psp_settlement_avro_contract(small_sim_bundle: SimulationBundle) -> None:
    for line in small_sim_bundle.psp_settlements[:25]:
        ok, err = validate_avro_record(line, "psp_settlement_event.v1.avsc")
        assert ok, err
