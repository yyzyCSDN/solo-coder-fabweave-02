from __future__ import annotations

import pytest

from fabweave.core.events import EventLog
from fabweave.genealogy.carrier import GenealogyGraph
from fabweave.holds.release import HoldState
from fabweave.transfer.chain import (
    STAGE_ARRIVE,
    STAGE_DEPART,
    STAGE_LOAD,
    STAGE_UNLOAD,
    transfer_links,
    transfer_scan,
    transfer_state,
)
from fabweave.workflows.control import attach_transfer_lineage, record_transfer_scan


CARRIERS = ["C1", "C2"]


def _complete_chain(log: EventLog, shipment: str = "T1", *, at: int = 100) -> None:
    assert transfer_scan(log, shipment, STAGE_LOAD, at, ["C1"]).allowed
    assert transfer_scan(log, shipment, STAGE_LOAD, at + 1, ["C2"]).allowed
    assert transfer_scan(log, shipment, STAGE_DEPART, at + 2).allowed
    assert transfer_scan(log, shipment, STAGE_ARRIVE, at + 3, CARRIERS).allowed
    assert transfer_scan(log, shipment, STAGE_UNLOAD, at + 4, CARRIERS).allowed


def test_scan_chain_must_advance_in_order():
    log = EventLog()
    decision = transfer_scan(log, "T1", STAGE_DEPART, 10)
    assert not decision.allowed
    assert decision.reasons == ("stage_out_of_order:DEPART:LOAD",)

    transfer_scan(log, "T1", STAGE_LOAD, 10, CARRIERS)
    # 跳过出厂直接到厂
    decision = transfer_scan(log, "T1", STAGE_ARRIVE, 11, CARRIERS)
    assert decision.reasons == ("stage_out_of_order:ARRIVE:DEPART",)
    assert decision.evidence["completed_stage"] == STAGE_LOAD
    # 被拒绝的扫描没有记账：链仍停在装载
    assert transfer_state(log, "T1").stage == STAGE_LOAD


def test_happy_path_records_one_event_per_stage_and_completes():
    log = EventLog()
    _complete_chain(log)
    state = transfer_state(log, "T1")
    assert state.stage == STAGE_UNLOAD and state.complete
    kinds = [event.kind for event in log.read("transfer:T1")]
    assert kinds == [
        "transfer_load_scanned",
        "transfer_load_scanned",
        "transfer_depart_scanned",
        "transfer_arrive_scanned",
        "transfer_unload_scanned",
    ]
    assert transfer_links(state) == (("C1", "C1"), ("C2", "C2"))


def test_rescanning_a_completed_stage_is_idempotent_and_not_booked_again():
    log = EventLog()
    _complete_chain(log)
    events_before = len(log.read("transfer:T1"))

    replay = transfer_scan(log, "T1", STAGE_DEPART, 200)
    assert replay.allowed and replay.evidence["replay"] is True
    assert replay.evidence["event_sequence"] == 3

    replay_arrival = transfer_scan(log, "T1", STAGE_ARRIVE, 201, ["C2", "C1"])
    assert replay_arrival.evidence["replay"] is True

    # 同批载体原样重扫装载也不重复记账（顺序不同仍是同一批）
    replay_load = transfer_scan(log, "T1", STAGE_LOAD, 202, ["C2"])
    assert replay_load.evidence["replay"] is True

    assert len(log.read("transfer:T1")) == events_before


def test_rescan_with_different_content_is_rejected_without_booking():
    log = EventLog()
    _complete_chain(log)
    events_before = len(log.read("transfer:T1"))
    decision = transfer_scan(log, "T1", STAGE_ARRIVE, 200, ["C1"])
    assert decision.reasons == ("duplicate_scan_content_changed",)
    assert decision.evidence["recorded_carriers"] == ("C1", "C2")
    assert len(log.read("transfer:T1")) == events_before


def test_reloading_an_already_loaded_carrier_is_rejected_but_new_batch_loads():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1"])
    decision = transfer_scan(log, "T1", STAGE_LOAD, 11, ["C1", "C2"])
    assert decision.reasons == ("carrier_already_loaded:C1",)
    assert transfer_state(log, "T1").loaded_carriers == frozenset({"C1"})

    assert transfer_scan(log, "T1", STAGE_LOAD, 12, ["C2"]).allowed
    assert transfer_state(log, "T1").loaded_carriers == frozenset({"C1", "C2"})


def test_arrival_with_missing_or_foreign_carrier_explains_the_mismatch():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, CARRIERS)
    transfer_scan(log, "T1", STAGE_DEPART, 11)
    decision = transfer_scan(log, "T1", STAGE_ARRIVE, 12, ["C1", "X9"])
    assert decision.reasons == ("arrival_carrier_mismatch",)
    assert decision.evidence["missing_carriers"] == ("C2",)
    assert decision.evidence["unexpected_carriers"] == ("X9",)
    assert transfer_state(log, "T1").stage == STAGE_DEPART


def test_unload_quantity_must_match_loaded_manifest():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, CARRIERS)
    transfer_scan(log, "T1", STAGE_DEPART, 11)
    transfer_scan(log, "T1", STAGE_ARRIVE, 12, CARRIERS)
    decision = transfer_scan(log, "T1", STAGE_UNLOAD, 13, ["C1"])
    assert decision.reasons == ("unload_carrier_mismatch",)
    assert decision.evidence["missing_carriers"] == ("C2",)
    assert not transfer_state(log, "T1").complete


def test_active_hold_blocks_departure_at_domain_level():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, CARRIERS)
    decision = transfer_scan(
        log, "T1", STAGE_DEPART, 11, active_holds={"C2": frozenset({"HOLD-QA"})}
    )
    assert decision.reasons == ("active_hold:HOLD-QA",)
    assert transfer_state(log, "T1").stage == STAGE_LOAD
    # 扣留解除后可以出厂
    assert transfer_scan(log, "T1", STAGE_DEPART, 12).allowed


def test_workflow_entry_blocks_departure_while_hold_is_open_and_releases_after():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, CARRIERS)
    holds = {"C2": HoldState("HOLD-QA", 1, frozenset({"QA"}))}

    blocked = record_transfer_scan(log, "T1", STAGE_DEPART, 11, holds=holds, expected_hold_versions={"C2": 1})
    assert "active_hold:HOLD-QA" in blocked.reasons
    assert "missing_disposition:QA@C2" in blocked.reasons
    assert transfer_state(log, "T1").stage == STAGE_LOAD

    # 处置完成、版本前进后放行
    released_holds = {"C2": HoldState("HOLD-QA", 2, frozenset({"QA"}), frozenset({"QA"}))}
    decision = record_transfer_scan(
        log, "T1", STAGE_DEPART, 12, holds=released_holds, expected_hold_versions={"C2": 2}
    )
    assert decision.allowed
    assert transfer_state(log, "T1").stage == STAGE_DEPART


def test_stale_hold_version_is_fail_closed():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, CARRIERS)
    holds = {"C1": HoldState("H1", 2, frozenset())}
    decision = record_transfer_scan(log, "T1", STAGE_DEPART, 11, holds=holds, expected_hold_versions={"C1": 1})
    assert "active_hold:H1" in decision.reasons
    assert "stale_hold_version@C1" in decision.reasons


def test_successful_transfer_reconnects_to_original_genealogy():
    log = EventLog()
    # 来源谱系：WAFER -> C1（C1 的上游还有工艺祖先）
    graph = GenealogyGraph({"WAFER": None, "C1": "WAFER", "C2": "WAFER"})
    transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1", "C2"])
    transfer_scan(log, "T1", STAGE_DEPART, 11)
    transfer_scan(log, "T1", STAGE_ARRIVE, 12, ["C1", "C2"])
    # 目的地用新载体号接收，谱系映射回来源载体
    decision = transfer_scan(
        log, "T1", STAGE_UNLOAD, 13, ["D1", "D2"], lineage={"D1": "C1", "D2": "C2"}
    )
    assert decision.allowed

    state = transfer_state(log, "T1")
    linked = attach_transfer_lineage(graph, state)

    # 沿新载体向上，仍能穿过转运段追溯到原谱系（C1 -> WAFER）
    upstream_d1 = linked.related("D1", "upstream", 5)
    assert {"C1", "WAFER"}.issubset(upstream_d1)
    assert any(node.startswith("XFER-") for node in upstream_d1)
    assert {"C2", "WAFER"}.issubset(linked.related("D2", "upstream", 5))


def test_identity_lineage_keeps_carrier_in_original_graph():
    log = EventLog()
    graph = GenealogyGraph({"WAFER": None, "C1": "WAFER", "C2": "WAFER"})
    _complete_chain(log)
    linked = attach_transfer_lineage(graph, transfer_state(log, "T1"))
    # 同名直连不插入转运段，原祖先关系不变
    assert linked.related("C1", "upstream", 5) == ("WAFER",)


def test_lineage_attach_is_idempotent():
    log = EventLog()
    graph = GenealogyGraph({"C1": None})
    transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1"])
    transfer_scan(log, "T1", STAGE_DEPART, 11)
    transfer_scan(log, "T1", STAGE_ARRIVE, 12, ["C1"])
    transfer_scan(log, "T1", STAGE_UNLOAD, 13, ["D1"], lineage={"D1": "C1"})
    state = transfer_state(log, "T1")
    once = attach_transfer_lineage(graph, state)
    twice = attach_transfer_lineage(once, state)
    assert twice.parents() == once.parents()
    upstream = twice.related("D1", "upstream", 5)
    assert "C1" in upstream and any(node.startswith("XFER-") for node in upstream)


def test_lineage_cannot_attach_before_chain_completes():
    log = EventLog()
    graph = GenealogyGraph({"C1": None})
    transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1"])
    with pytest.raises(ValueError):
        attach_transfer_lineage(graph, transfer_state(log, "T1"))


def test_lineage_origin_must_come_from_loaded_manifest():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1"])
    transfer_scan(log, "T1", STAGE_DEPART, 11)
    transfer_scan(log, "T1", STAGE_ARRIVE, 12, ["C1"])
    decision = transfer_scan(log, "T1", STAGE_UNLOAD, 13, ["D1"], lineage={"D1": "GHOST"})
    assert decision.reasons == ("lineage_origin_not_loaded:GHOST",)
    assert not transfer_state(log, "T1").complete


def test_duplicate_carrier_in_one_scan_is_rejected():
    log = EventLog()
    decision = transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1", "C1"])
    assert decision.reasons == ("duplicate_carrier_in_scan",)
    assert log.read("transfer:T1") == ()


def test_backdated_scan_is_rejected_by_the_event_log():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 20, ["C1"])
    with pytest.raises(ValueError):
        transfer_scan(log, "T1", STAGE_DEPART, 19)


def test_attach_rejects_destination_already_on_another_parent():
    log = EventLog()
    transfer_scan(log, "T1", STAGE_LOAD, 10, ["C1"])
    transfer_scan(log, "T1", STAGE_DEPART, 11)
    transfer_scan(log, "T1", STAGE_ARRIVE, 12, ["C1"])
    transfer_scan(log, "T1", STAGE_UNLOAD, 13, ["D1"], lineage={"D1": "C1"})
    graph = GenealogyGraph({"C1": None, "D1": "OTHER"})
    with pytest.raises(ValueError):
        attach_transfer_lineage(graph, transfer_state(log, "T1"))
