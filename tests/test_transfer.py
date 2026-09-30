from __future__ import annotations

import pytest

from fabweave.core.decision import allow
from fabweave.core.events import EventLog
from fabweave.genealogy.carrier import GenealogyGraph
from fabweave.holds.release import HoldState
from fabweave.shipment.transfer import STAGES, TransferLedger
from fabweave.workflows.control import cross_factory_transfer


@pytest.fixture
def pedigree() -> GenealogyGraph:
    # WAFER -> LOT-A : the lot carries a wafer ancestry across the transfer.
    return GenealogyGraph({"WAFER-1": None, "LOT-A": "WAFER-1"})


@pytest.fixture
def ledger() -> TransferLedger:
    return TransferLedger()


def scan_chain(
    ledger: TransferLedger,
    pedigree: GenealogyGraph,
    transfer_id: str = "XFER-1",
    lot_id: str = "LOT-A",
    quantity: int = 10,
    hold: HoldState | None = None,
    received_quantity: int | None = None,
    origin_graph: GenealogyGraph | None = None,
    destination_graph: GenealogyGraph | None = None,
):
    origin_graph = origin_graph if origin_graph is not None else pedigree
    destination_graph = destination_graph if destination_graph is not None else pedigree
    return (
        ledger.load(transfer_id, lot_id, quantity, "E-LOAD", 10, "FAB-A", "FAB-B", origin_graph),
        ledger.depart(transfer_id, "E-DEPART", 20, hold),
        ledger.arrive(transfer_id, "E-ARRIVE", 30),
        ledger.unload(transfer_id, "E-UNLOAD", 40, destination_graph, received_quantity),
    )


def test_chain_advances_in_order_and_ends_unloaded(ledger, pedigree):
    decisions = scan_chain(ledger, pedigree)
    assert [decision.allowed for decision in decisions] == [True, True, True, True]
    transfer = ledger.get("XFER-1")
    assert transfer.stage == "unloaded"
    assert transfer.event_count == 4
    assert STAGES == ("loaded", "departed", "arrived", "unloaded")


def test_repeated_scan_does_not_book_a_second_event(ledger, pedigree):
    scan_chain(ledger, pedigree)
    before = ledger.get("XFER-1").event_count
    # Re-scan every stage with fresh scan ids: all replay, no new events.
    replays = [
        ledger.load("XFER-1", "LOT-A", 10, "E-LOAD-2", 11, "FAB-A", "FAB-B", pedigree),
        ledger.depart("XFER-1", "E-DEPART-2", 21),
        ledger.arrive("XFER-1", "E-ARRIVE-2", 31),
        ledger.unload("XFER-1", "E-UNLOAD-2", 41, pedigree),
    ]
    assert all(decision.allowed and decision.evidence["replay"] for decision in replays)
    assert ledger.get("XFER-1").event_count == before


def test_same_scan_id_is_idempotent_in_the_event_log(ledger, pedigree):
    first = ledger.load("XFER-1", "LOT-A", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    replay = ledger.load("XFER-1", "LOT-A", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    assert first.allowed and replay.allowed and ledger.get("XFER-1").event_count == 1
    # Reusing a scan id with different content is a hard conflict.
    with pytest.raises(ValueError):
        ledger.depart("XFER-1", "E-LOAD", 20)


def test_skipping_a_step_is_denied_with_the_missing_step(ledger, pedigree):
    ledger.load("XFER-1", "LOT-A", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    arrive = ledger.arrive("XFER-1", "E-ARRIVE", 30)
    assert not arrive.allowed
    assert arrive.reasons == ("stage_skipped:arrived_requires:departed",)
    assert ledger.get("XFER-1").stage == "loaded"
    unload = ledger.unload("XFER-1", "E-UNLOAD", 40, pedigree)
    assert unload.reasons == ("stage_skipped:unloaded_requires:departed",)


def test_scanning_before_load_is_denied(ledger):
    depart = ledger.depart("XFER-9", "E-DEPART", 20)
    assert not depart.allowed
    assert depart.reasons == ("stage_skipped:departed_requires:loaded",)


def test_active_hold_blocks_departure(ledger, pedigree):
    ledger.load("XFER-1", "LOT-A", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    hold = HoldState("H-1", 1, frozenset({"QA"}), frozenset(), frozenset({"CUSTOMER"}))
    depart = ledger.depart("XFER-1", "E-DEPART", 20, hold)
    assert not depart.allowed
    assert "active_blocker:CUSTOMER" in depart.reasons
    assert "missing_disposition:QA" in depart.reasons
    assert ledger.get("XFER-1").stage == "loaded"


def test_released_hold_does_not_block_departure(ledger, pedigree):
    ledger.load("XFER-1", "LOT-A", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    hold = HoldState("H-1", 1, frozenset({"QA"}), frozenset({"QA"}), frozenset(), released_at=15)
    assert ledger.depart("XFER-1", "E-DEPART", 20, hold).allowed
    assert ledger.get("XFER-1").stage == "departed"


def test_lot_without_genealogy_cannot_load(ledger):
    pedigree = GenealogyGraph({"WAFER-1": None})
    decision = ledger.load("XFER-1", "LOT-X", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    assert not decision.allowed
    assert decision.reasons == ("lot_not_in_genealogy",)


def test_unload_reconnects_the_unchanged_pedigree(ledger, pedigree):
    scan_chain(ledger, pedigree)
    transfer = ledger.get("XFER-1")
    assert transfer.ancestry == ("WAFER-1",)
    events = ledger._log.read("transfer:XFER-1")
    assert events[0].data["ancestry"] == ["WAFER-1"]
    assert events[-1].data["ancestry"] == ["WAFER-1"]


def test_broken_pedigree_at_destination_blocks_unload(ledger, pedigree):
    scan_chain(ledger, pedigree, destination_graph=GenealogyGraph({"WAFER-1": None, "LOT-A": "WAFER-2"}))
    # The chain stopped at arrive; unload must refuse to reconnect.
    assert ledger.get("XFER-1").stage == "arrived"
    unload = ledger.unload("XFER-1", "E-UNLOAD", 40, GenealogyGraph({"WAFER-1": None, "LOT-A": "WAFER-2"}))
    assert not unload.allowed
    assert unload.reasons == ("genealogy_disconnected",)


def test_received_quantity_mismatch_is_denied(ledger, pedigree):
    scan_chain(ledger, pedigree, received_quantity=9)
    assert ledger.get("XFER-1").stage == "arrived"
    unload = ledger.unload("XFER-1", "E-UNLOAD", 40, pedigree, received_quantity=9)
    assert not unload.allowed
    assert unload.reasons == ("quantity_mismatch",)


def test_backdated_scan_is_rejected_by_the_event_log(ledger, pedigree):
    ledger.load("XFER-1", "LOT-A", 10, "E-LOAD", 10, "FAB-A", "FAB-B", pedigree)
    ledger.depart("XFER-1", "E-DEPART", 20)
    with pytest.raises(ValueError):
        ledger.arrive("XFER-1", "E-ARRIVE-OLD", 5)


def test_workflow_runs_the_chain_and_stops_at_the_hold(ledger, pedigree):
    hold = HoldState("H-1", 1, frozenset({"QA"}), frozenset(), frozenset({"CUSTOMER"}))
    decision = cross_factory_transfer(
        ledger,
        "XFER-1",
        "LOT-A",
        10,
        ("E-LOAD", "E-DEPART", "E-ARRIVE", "E-UNLOAD"),
        (10, 20, 30, 40),
        "FAB-A",
        "FAB-B",
        pedigree,
        pedigree,
        hold,
    )
    assert not decision.allowed
    assert "active_blocker:CUSTOMER" in decision.reasons
    assert ledger.get("XFER-1").stage == "loaded"


def test_workflow_completes_the_chain(ledger, pedigree):
    decision = cross_factory_transfer(
        ledger,
        "XFER-1",
        "LOT-A",
        10,
        ("E-LOAD", "E-DEPART", "E-ARRIVE", "E-UNLOAD"),
        (10, 20, 30, 40),
        "FAB-A",
        "FAB-B",
        pedigree,
        pedigree,
    )
    assert decision.allowed and decision.evidence["stage"] == "unloaded"
    assert ledger.get("XFER-1").stage == "unloaded"
