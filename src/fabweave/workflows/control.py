from __future__ import annotations

from dataclasses import dataclass, replace

from fabweave.chamber.matching import Tolerance, mismatch_reasons
from fabweave.chemicals.shelf_life import shelf_life_remaining
from fabweave.cleanroom.excursion import active_excursion
from fabweave.contamination.segregation import incompatible_pairs
from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import EventLog
from fabweave.core.ids import deterministic_id
from fabweave.core.timebox import TimeBox
from fabweave.dedication.tool import DedicationPolicy, policy_decision
from fabweave.dispatch.priority import DispatchCandidate, rank_candidates
from fabweave.energy.demand import demand_violations
from fabweave.genealogy.carrier import GenealogyGraph
from fabweave.holds.release import HoldState, release_decision
from fabweave.lot.merge import Lot, merge_lots
from fabweave.maintenance.window import MaintenancePolicy, assignment_decision
from fabweave.metrology.sampling import Wafer, deterministic_sample
from fabweave.queue_time.enforcer import QueueLimit, queue_time_decision
from fabweave.recipe.qualification import Qualification, qualification_decision
from fabweave.reticle.reservation import Reservation, ReservationBook
from fabweave.rework.authorization import ReworkPolicy, authorization_decision
from fabweave.shipment.commit import Allocation, CommitLedger
from fabweave.spc.control import western_electric_rules
from fabweave.takt.monitor import TaktResult, takt_result
from fabweave.transfer.chain import (
    STAGE_DEPART,
    TransferState,
    transfer_scan,
    transfer_state,
)
from fabweave.yielding.rollup import StepYield, route_yield


def _combine(decisions: list[Decision], **evidence: object) -> Decision:
    reasons = tuple(reason for decision in decisions for reason in decision.reasons)
    merged = {key: value for decision in decisions for key, value in decision.evidence.items()}
    merged.update(evidence)
    return deny(*reasons, **merged) if reasons else allow(**merged)


def material_readiness(
    queue_limit: QueueLimit,
    entered_at: int,
    evaluated_at: int,
    queue_pauses: list[TimeBox],
    opened_at: int,
    shelf_hours: int,
    frozen_periods: list[TimeBox],
) -> Decision:
    queue = queue_time_decision(queue_limit, entered_at, evaluated_at, queue_pauses)
    remaining = shelf_life_remaining(opened_at, shelf_hours, evaluated_at, frozen_periods)
    chemical = deny("chemical_shelf_life_expired", remaining_shelf_hours=remaining) if remaining < 0 else allow(remaining_shelf_hours=remaining)
    return _combine([queue, chemical])


def rank_releasable(
    candidates: list[DispatchCandidate],
    now: int,
    holds: dict[str, HoldState],
    expected_versions: dict[str, int],
) -> list[DispatchCandidate]:
    evaluated: list[DispatchCandidate] = []
    for candidate in candidates:
        state = holds.get(candidate.lot_id)
        blocked = candidate.active_hold
        if state is not None:
            expected = expected_versions.get(candidate.lot_id, state.version)
            blocked = blocked or not release_decision(state, expected, now).allowed
        evaluated.append(replace(candidate, active_hold=blocked))
    return rank_candidates(evaluated, now)


def reserve_qualified(
    book: ReservationBook,
    request_id: str,
    requested: list[Reservation],
    assignments: dict[str, tuple[str, str]],
    qualifications: list[Qualification],
    at: int,
) -> Decision:
    decisions: list[Decision] = []
    for reservation in requested:
        assignment = assignments.get(reservation.job_id)
        if assignment is None:
            decisions.append(deny(f"assignment_missing:{reservation.job_id}"))
            continue
        tool_id, chamber_id = assignment
        decisions.append(qualification_decision(tool_id, chamber_id, reservation.recipe_revision, at, qualifications))
    combined = _combine(decisions, request_id=request_id)
    if not combined.allowed:
        return combined
    reserved = book.reserve_batch(request_id, requested)
    return allow(request_id=request_id, reservation_count=len(reserved))


def qualified_assignment(
    policy: MaintenancePolicy,
    last_pm_at: float,
    accumulated_run_hours: float,
    now: int,
    job_window: TimeBox,
    planned_maintenance: list[TimeBox],
    tool_id: str,
    chamber_id: str,
    recipe_revision: int,
    qualifications: list[Qualification],
) -> Decision:
    maintenance = assignment_decision(policy, last_pm_at, accumulated_run_hours, now, job_window, planned_maintenance)
    qualification = qualification_decision(tool_id, chamber_id, recipe_revision, job_window.start, qualifications)
    return _combine([maintenance, qualification])


@dataclass(frozen=True, slots=True)
class SampleEvaluation:
    wafer_ids: tuple[str, ...]
    rules: tuple[str, ...]


def evaluate_sample(
    wafers: list[Wafer],
    sample_count: int,
    required_zones: set[str],
    measurements: dict[str, float],
    center: float,
    sigma: float,
) -> SampleEvaluation:
    selected = deterministic_sample(wafers, sample_count, required_zones)
    missing = sorted(wafer.wafer_id for wafer in selected if wafer.wafer_id not in measurements)
    if missing:
        raise ValueError(f"sample measurements missing: {', '.join(missing)}")
    values = [measurements[wafer.wafer_id] for wafer in selected]
    return SampleEvaluation(tuple(wafer.wafer_id for wafer in selected), western_electric_rules(values, center, sigma))


def chamber_sample_decision(
    reference: dict[str, float],
    candidate: dict[str, float],
    tolerances: dict[str, Tolerance],
    wafers: list[Wafer],
    sample_count: int,
    required_zones: set[str],
) -> Decision:
    reasons = list(mismatch_reasons(reference, candidate, tolerances))
    try:
        sample = deterministic_sample(wafers, sample_count, required_zones)
    except ValueError as error:
        reasons.append(f"sample:{error}")
        sample = ()
    evidence = {"sample_wafer_ids": tuple(wafer.wafer_id for wafer in sample)}
    return deny(*reasons, **evidence) if reasons else allow(**evidence)


def environmental_containment(
    readings: list[tuple[int, float]],
    open_limit: float,
    close_limit: float,
    required_open: int,
    required_close: int,
    material_classes: set[str],
    contamination_matrix: dict[str, set[str]],
) -> Decision:
    excursion = active_excursion(readings, open_limit, close_limit, required_open, required_close)
    pairs = incompatible_pairs(material_classes, contamination_matrix)
    reasons = [*(("cleanroom_excursion",) if excursion else ()), *(f"incompatible:{left}:{right}" for left, right in pairs)]
    return deny(*reasons, active_excursion=excursion, incompatible_pairs=pairs) if reasons else allow(active_excursion=False, incompatible_pairs=())


def containment_scope(graph: GenealogyGraph, seeds: list[str], max_depth: int) -> tuple[str, ...]:
    impacted = set(seeds)
    for seed in seeds:
        impacted.update(graph.related(seed, "upstream", max_depth))
        impacted.update(graph.related(seed, "downstream", max_depth))
    return tuple(sorted(impacted))


@dataclass(frozen=True, slots=True)
class ContainmentRecord:
    scope: tuple[str, ...]
    event_sequence: int


def record_containment(
    log: EventLog,
    request_id: str,
    graph: GenealogyGraph,
    seeds: list[str],
    max_depth: int,
    at: int,
) -> ContainmentRecord:
    scope = containment_scope(graph, seeds, max_depth)
    event = log.append(
        f"containment:{request_id}",
        0,
        request_id,
        at,
        "containment_planned",
        {"scope": list(scope)},
    )
    return ContainmentRecord(tuple(event.data["scope"]), event.sequence)


def powered_assignment(
    loads: list[tuple[int, float]],
    demand_windows: list[tuple[int, float]],
    maintenance_policy: MaintenancePolicy,
    last_pm_at: float,
    run_hours: float,
    now: int,
    job_window: TimeBox,
    maintenance_windows: list[TimeBox],
) -> Decision:
    widths = demand_violations(loads, demand_windows)
    demand = deny(*(f"demand_limit_exceeded:{width}" for width in widths), violated_windows=widths) if widths else allow(violated_windows=())
    maintenance = assignment_decision(maintenance_policy, last_pm_at, run_hours, now, job_window, maintenance_windows)
    return _combine([demand, maintenance])


def rework_release_decision(
    policy: ReworkPolicy,
    operation: str,
    prior_reworks: int,
    defect_code: str,
    engineer_approval: str | None,
    hold: HoldState,
    expected_hold_version: int,
    at: int,
) -> Decision:
    authorization = authorization_decision(policy, operation, prior_reworks, defect_code, engineer_approval)
    release = release_decision(hold, expected_hold_version, at)
    return _combine([authorization, release])


def merge_and_commit(request_id: str, lots: list[Lot], ledger: CommitLedger, order_id: str) -> tuple[Lot, Allocation]:
    merged = merge_lots(request_id, lots)
    allocation = ledger.allocate(request_id, order_id, merged.quantity)
    return merged, allocation


def dedicated_qualification(
    product: str,
    tool_id: str,
    chamber_id: str,
    recipe_revision: int,
    at: int,
    dedication_policies: list[DedicationPolicy],
    qualifications: list[Qualification],
) -> Decision:
    dedication = policy_decision(product, tool_id, at, dedication_policies)
    qualification = qualification_decision(tool_id, chamber_id, recipe_revision, at, qualifications)
    return _combine([dedication, qualification])


def departure_holds(
    holds: dict[str, HoldState],
    expected_versions: dict[str, int],
    carriers: frozenset[str],
    at: int,
) -> tuple[dict[str, frozenset[str]], tuple[str, ...]]:
    """出厂时刻仍处于活动状态的扣留。

    复用扣留域的放行判断：版本过期、处置未完成、活动 blocker 都算活动扣留。
    返回 ``(载体 -> 扣留标识集合, 细化原因)``，细化原因形如
    ``missing_disposition:QA@C1``，保留扣留域给出的原始拒绝原因。
    """
    active: dict[str, frozenset[str]] = {}
    reasons: list[str] = []
    for carrier in sorted(carriers):
        state = holds.get(carrier)
        if state is None:
            continue
        expected = expected_versions.get(carrier, state.version)
        decision = release_decision(state, expected, at)
        if decision.allowed:
            continue
        active[carrier] = frozenset({state.hold_id})
        reasons.extend(f"{reason}@{carrier}" for reason in decision.reasons)
    return active, tuple(reasons)


def record_transfer_scan(
    log: EventLog,
    shipment_id: str,
    stage: str,
    at: int,
    carriers: list[str] | tuple[str, ...] = (),
    holds: dict[str, HoldState] | None = None,
    expected_hold_versions: dict[str, int] | None = None,
    lineage: dict[str, str] | None = None,
) -> Decision:
    """跨厂转运扫描入口，出厂阶段接入活动扣留判断。

    与 :func:`fabweave.transfer.chain.transfer_scan` 行为一致，但在出厂时
    额外通过扣留域判断每个载体；有活动扣留则以
    ``active_hold:<扣留标识>``（并附扣留域细化原因）拒绝，不写事件。
    """
    active_holds: dict[str, frozenset[str]] | None = None
    hold_reasons: tuple[str, ...] = ()
    if stage == STAGE_DEPART:
        state = transfer_state(log, shipment_id)
        active_holds, hold_reasons = departure_holds(
            holds or {}, expected_hold_versions or {}, state.loaded_carriers, at
        )
    decision = transfer_scan(
        log, shipment_id, stage, at, carriers, active_holds=active_holds, lineage=lineage
    )
    if decision.allowed:
        return decision
    if not any(reason.startswith("active_hold:") for reason in decision.reasons):
        return decision
    return deny(*decision.reasons, *hold_reasons, **dict(decision.evidence))


def attach_transfer_lineage(
    graph: GenealogyGraph,
    state: TransferState,
) -> GenealogyGraph:
    """转运成功后把目的地载体接回原有物料谱系。

    仅对卸载已完成（链完整）的转运生效。每条非同名谱系链接插入一个唯一的
    转运段节点：``来源载体 -> XFER-<转运单>-<目的地载体> -> 目的地载体``，
    因此沿目的地载体向上仍能追溯到转运前的全部祖先；同名直连不改动谱系。
    重复接入同一条转运是幂等的；目的地载体已有不同父节点或接入后成环则报错。
    """
    if not state.complete:
        raise ValueError("transfer chain is not complete")
    parent_of = graph.parents()
    for destination, origin in state.lineage_links:
        if destination == origin:
            # 同名直连：载体以原标识继续存在，谱系图保持原样即可。
            parent_of.setdefault(destination, None)
            continue
        segment = deterministic_id("XFER", state.shipment_id, origin, destination, length=10)
        if segment not in parent_of:
            parent_of.setdefault(origin, None)
            parent_of[segment] = origin
        if parent_of.get(destination) not in (None, segment):
            raise ValueError("destination carrier already attached to a different parent")
        parent_of[destination] = segment
    return GenealogyGraph(parent_of)


@dataclass(frozen=True, slots=True)
class PerformanceSummary:
    takt: TaktResult
    route_yield: float


def performance_summary(
    completions: list[int],
    window_start: int,
    window_end: int,
    steps: list[StepYield],
    expected_steps: list[str],
) -> PerformanceSummary:
    return PerformanceSummary(takt_result(completions, window_start, window_end), route_yield(steps, expected_steps))
