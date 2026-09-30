"""Cross-factory transfer scan chain.

A transfer moves material between factories through four ordered scans:
``loaded -> departed -> arrived -> unloaded``.  The chain is trusted
because every scan is an idempotent event in an append-only log:

* scans must advance in order -- a skip is denied with the missing step
  named, never silently accepted;
* replaying a scan (same scan id, or the same stage scanned twice) returns
  the original decision and never books a second event;
* an active hold gates departure, so held material cannot leave the origin;
* the lot's genealogy is snapshotted at load and re-checked at unload, so
  the pedigree stays connected across the transfer.
"""

from __future__ import annotations

from dataclasses import dataclass

from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import EventLog
from fabweave.core.quantities import require_conservation
from fabweave.genealogy.carrier import GenealogyGraph
from fabweave.holds.release import HoldState, release_decision


STAGES: tuple[str, ...] = ("loaded", "departed", "arrived", "unloaded")
_STAGE_INDEX = {stage: index for index, stage in enumerate(STAGES)}


@dataclass(frozen=True, slots=True)
class Transfer:
    transfer_id: str
    lot_id: str
    origin_factory: str
    destination_factory: str
    quantity: int
    stage: str
    ancestry: tuple[str, ...]
    event_count: int


@dataclass(slots=True)
class _TransferState:
    transfer_id: str
    lot_id: str
    origin_factory: str
    destination_factory: str
    quantity: int
    ancestry: tuple[str, ...]
    stage: str


class TransferLedger:
    """Append-only scan chain for cross-factory material transfers."""

    def __init__(self, log: EventLog | None = None) -> None:
        self._log = log if log is not None else EventLog()
        self._transfers: dict[str, _TransferState] = {}

    def get(self, transfer_id: str) -> Transfer:
        state = self._require(transfer_id)
        stream = f"transfer:{transfer_id}"
        return Transfer(
            state.transfer_id,
            state.lot_id,
            state.origin_factory,
            state.destination_factory,
            state.quantity,
            state.stage,
            state.ancestry,
            len(self._log.read(stream)),
        )

    def load(
        self,
        transfer_id: str,
        lot_id: str,
        quantity: int,
        event_id: str,
        at: int,
        origin_factory: str,
        destination_factory: str,
        graph: GenealogyGraph,
    ) -> Decision:
        """Scan the lot onto the vehicle at the origin factory."""

        if quantity <= 0:
            raise ValueError("transfer quantity must be positive")
        existing = self._transfers.get(transfer_id)
        if existing is not None:
            return allow(replay=True, stage=existing.stage)
        if lot_id not in graph:
            return deny("lot_not_in_genealogy", lot_id=lot_id)
        state = _TransferState(
            transfer_id,
            lot_id,
            origin_factory,
            destination_factory,
            quantity,
            graph.ancestry_of(lot_id),
            "loaded",
        )
        self._transfers[transfer_id] = state
        return self._record(
            state,
            "loaded",
            event_id,
            at,
            {
                "lot_id": lot_id,
                "quantity": quantity,
                "origin_factory": origin_factory,
                "destination_factory": destination_factory,
                "ancestry": list(state.ancestry),
            },
        )

    def depart(
        self,
        transfer_id: str,
        event_id: str,
        at: int,
        hold: HoldState | None = None,
        expected_hold_version: int | None = None,
    ) -> Decision:
        """Scan the vehicle out of the origin factory.

        An active hold (missing dispositions or active blockers) denies
        departure with the hold's reasons.
        """

        state = self._transfers.get(transfer_id)
        order = self._check_order(state, "departed")
        if order is not None:
            return order
        if hold is not None:
            expected = expected_hold_version if expected_hold_version is not None else hold.version
            release = release_decision(hold, expected, at)
            if not release.allowed:
                evidence = dict(release.evidence)
                evidence["stage"] = "departed"
                return deny(*release.reasons, **evidence)
        return self._record(state, "departed", event_id, at, {"lot_id": state.lot_id, "quantity": state.quantity})

    def arrive(self, transfer_id: str, event_id: str, at: int) -> Decision:
        """Scan the vehicle into the destination factory."""

        state = self._transfers.get(transfer_id)
        order = self._check_order(state, "arrived")
        if order is not None:
            return order
        return self._record(state, "arrived", event_id, at, {"lot_id": state.lot_id, "quantity": state.quantity})

    def unload(
        self,
        transfer_id: str,
        event_id: str,
        at: int,
        graph: GenealogyGraph,
        received_quantity: int | None = None,
    ) -> Decision:
        """Scan the lot off the vehicle at the destination factory.

        Confirms the received quantity and re-anchors the lot's genealogy
        against the destination graph: the pedigree must be unchanged.
        """

        state = self._transfers.get(transfer_id)
        order = self._check_order(state, "unloaded")
        if order is not None:
            return order
        received = state.quantity if received_quantity is None else received_quantity
        try:
            require_conservation({"loaded": state.quantity}, {"unloaded": received})
        except ValueError:
            return deny("quantity_mismatch", loaded=state.quantity, received=received)
        if state.lot_id not in graph or graph.ancestry_of(state.lot_id) != state.ancestry:
            return deny("genealogy_disconnected", lot_id=state.lot_id)
        return self._record(
            state,
            "unloaded",
            event_id,
            at,
            {
                "lot_id": state.lot_id,
                "quantity": state.quantity,
                "received_quantity": received,
                "ancestry": list(state.ancestry),
            },
        )

    def _require(self, transfer_id: str) -> _TransferState:
        state = self._transfers.get(transfer_id)
        if state is None:
            raise ValueError(f"unknown transfer: {transfer_id}")
        return state

    def _check_order(self, state: _TransferState | None, stage: str) -> Decision | None:
        if state is None:
            return deny(f"stage_skipped:{stage}_requires:loaded", stage=stage)
        requested = _STAGE_INDEX[stage]
        current = _STAGE_INDEX[state.stage]
        if requested <= current:
            return allow(replay=True, stage=state.stage)
        if requested > current + 1:
            expected = STAGES[current + 1]
            return deny(
                f"stage_skipped:{stage}_requires:{expected}",
                stage=stage,
                current_stage=state.stage,
            )
        return None

    def _record(self, state: _TransferState, stage: str, event_id: str, at: int, data: dict) -> Decision:
        stream = f"transfer:{state.transfer_id}"
        event = self._log.append(stream, len(self._log.read(stream)), event_id, at, stage, data)
        state.stage = stage
        return allow(stage=stage, event_sequence=event.sequence)
