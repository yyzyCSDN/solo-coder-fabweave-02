"""可信跨厂转运扫描链。

扫描必须按 ``LOAD``（装载）→ ``DEPART``（出厂）→ ``ARRIVE``（到厂）→
``UNLOAD``（卸载）四个阶段顺序推进。每次扫描把一个事件追加到该转运单独占的
事件流中；重复扫描由事件内容比对 + 事件日志幂等性保证不重复记账。

任何不符合阶段顺序的扫描都不会记账，并以 :class:`~fabweave.core.decision.Decision`
的原因（reasons）说明：

``stage_out_of_order:<扫描阶段>:<期望阶段>``
    跳步或回扫（例如未出厂先到厂、卸载后再扫出厂）。
``duplicate_scan_content_changed``
    重扫已完成阶段，但扫描内容与首次记录不一致。
``active_hold:<扣留标识>``
    出厂时承运载体上存在活动扣留。
``arrival_carrier_mismatch`` / ``unload_carrier_mismatch``
    到厂/卸载的载体集合与装载清单对不上（串链、丢件）。

重扫一个已完成阶段且内容一致时，决策带 ``replay=True`` 返回原事件，不追加、
不重复记账。

卸载成功后，谱系可以通过 :func:`attach_transfer_lineage` 把目的地载体接回原有
物料谱系，转运只是谱系中一个可追溯的中间节点。
"""

from __future__ import annotations

from dataclasses import dataclass

from fabweave.core.decision import Decision, allow, deny
from fabweave.core.events import Event, EventLog
from fabweave.core.ids import deterministic_id

STAGE_LOAD = "LOAD"
STAGE_DEPART = "DEPART"
STAGE_ARRIVE = "ARRIVE"
STAGE_UNLOAD = "UNLOAD"

#: 扫描链允许的推进顺序。
STAGE_ORDER = (STAGE_LOAD, STAGE_DEPART, STAGE_ARRIVE, STAGE_UNLOAD)

LOAD_SCANNED = "transfer_load_scanned"
DEPART_SCANNED = "transfer_depart_scanned"
ARRIVE_SCANNED = "transfer_arrive_scanned"
UNLOAD_SCANNED = "transfer_unload_scanned"

_STAGE_EVENT = {
    STAGE_LOAD: LOAD_SCANNED,
    STAGE_DEPART: DEPART_SCANNED,
    STAGE_ARRIVE: ARRIVE_SCANNED,
    STAGE_UNLOAD: UNLOAD_SCANNED,
}


@dataclass(frozen=True, slots=True)
class TransferState:
    """一条转运单扫描链的投影状态。"""

    shipment_id: str
    loaded_carriers: frozenset[str]
    departed: bool = False
    arrived: bool = False
    unloaded: bool = False
    destination_carriers: frozenset[str] = frozenset()
    lineage_links: tuple[tuple[str, str], ...] = ()

    @property
    def stage(self) -> str | None:
        """链上最后完成的阶段；尚未装载时为 ``None``。"""
        if self.unloaded:
            return STAGE_UNLOAD
        if self.arrived:
            return STAGE_ARRIVE
        if self.departed:
            return STAGE_DEPART
        if self.loaded_carriers:
            return STAGE_LOAD
        return None

    @property
    def complete(self) -> bool:
        return self.unloaded


def _stream(shipment_id: str) -> str:
    return f"transfer:{shipment_id}"


def transfer_state(log: EventLog, shipment_id: str) -> TransferState:
    """从事件日志投影出转运单当前状态（重放结果，不持有可变数据）。"""
    loaded: set[str] = set()
    departed = arrived = unloaded = False
    destinations: set[str] = set()
    links: set[tuple[str, str]] = set()
    for event in log.read(_stream(shipment_id)):
        if event.kind == LOAD_SCANNED:
            loaded.update(event.data["carriers"])
        elif event.kind == DEPART_SCANNED:
            departed = True
        elif event.kind == ARRIVE_SCANNED:
            arrived = True
        elif event.kind == UNLOAD_SCANNED:
            unloaded = True
            destinations.update(event.data["carriers"])
            links.update((dest, origin) for dest, origin in event.data["lineage"])
    return TransferState(
        shipment_id,
        frozenset(loaded),
        departed,
        arrived,
        unloaded,
        frozenset(destinations),
        tuple(sorted(links)),
    )


def _scan_event_id(shipment_id: str, stage: str, carriers: tuple[str, ...]) -> str:
    # 事件内容全部进入摘要：同一载体重复扫描得到同一事件 id（幂等），
    # 换了载体或阶段则是另一个事件。
    return deterministic_id("TXN", shipment_id, stage, *carriers)


def _recorded_event(log: EventLog, shipment_id: str, kind: str) -> Event | None:
    return next((event for event in log.read(_stream(shipment_id)) if event.kind == kind), None)


def _replay_or_changed(
    log: EventLog,
    shipment_id: str,
    stage: str,
    carriers: tuple[str, ...],
    lineage: dict[str, str] | None,
) -> Decision:
    """阶段已经完成：内容一致则重放，不一致则拒绝（都不记账）。"""
    original = _recorded_event(log, shipment_id, _STAGE_EVENT[stage])
    evidence = {"shipment_id": shipment_id, "stage": stage, "completed_stage": transfer_state(log, shipment_id).stage}
    if original is None:  # 理论上到不了：stage 已完成意味着事件存在
        return deny(f"stage_not_recorded:{stage}", **evidence)

    if stage == STAGE_DEPART:
        if carriers:
            return deny("duplicate_scan_content_changed", **evidence)
    elif stage == STAGE_LOAD:
        # 装载允许分批，重扫任一批都幂等；顺序不同视为同一批。
        batches = [frozenset(event.data["carriers"]) for event in log.read(_stream(shipment_id)) if event.kind == LOAD_SCANNED]
        if frozenset(carriers) not in batches:
            return deny(
                "duplicate_scan_content_changed",
                recorded_carriers=tuple(sorted(set().union(*batches))),
                rescanned_carriers=tuple(sorted(carriers)),
                **evidence,
            )
        original = next(event for event in log.read(_stream(shipment_id)) if event.kind == LOAD_SCANNED and frozenset(event.data["carriers"]) == frozenset(carriers))
        replay_evidence = dict(evidence)
        replay_evidence.update(event_sequence=original.sequence, replay=True, recorded_at=original.occurred_at)
        return allow(**replay_evidence)
    else:
        recorded = tuple(sorted(original.data["carriers"]))
        if tuple(sorted(carriers)) != recorded:
            return deny(
                "duplicate_scan_content_changed",
                recorded_carriers=recorded,
                rescanned_carriers=tuple(sorted(carriers)),
                **evidence,
            )
        if stage == STAGE_UNLOAD and lineage is not None:
            recorded_links = {dest: origin for dest, origin in original.data["lineage"]}
            if recorded_links != dict(lineage):
                return deny("duplicate_scan_content_changed", **evidence)

    replay_evidence = dict(evidence)
    replay_evidence.update(event_sequence=original.sequence, replay=True, recorded_at=original.occurred_at)
    if stage == STAGE_UNLOAD:
        replay_evidence["lineage_links"] = tuple(sorted((dest, origin) for dest, origin in original.data["lineage"]))
    return allow(**replay_evidence)


def transfer_scan(
    log: EventLog,
    shipment_id: str,
    stage: str,
    at: int,
    carriers: list[str] | tuple[str, ...] = (),
    active_holds: dict[str, frozenset[str]] | None = None,
    lineage: dict[str, str] | None = None,
) -> Decision:
    """记录一次扫描并推进扫描链。

    参数:
        log: 追加式事件日志，转运单按 ``transfer:<shipment_id>`` 独立成流。
        shipment_id: 转运单号。
        stage: ``STAGE_ORDER`` 中的扫描阶段。
        at: 事件时间（不得早于本转运单已有的扫描时间）。
        carriers: 本次扫描到的载体标识。
            - 装载：本批装上的载体，可分多次扫描补载；重复扫已装载体不记账；
            - 出厂：不需要载体；
            - 到厂：必须与累计装载清单完全一致；
            - 卸载：本次卸下的目的地载体，默认与来源载体同名，可用
              ``lineage`` 给出目的地载体到来源载体的映射，数量必须对得上。
        active_holds: 载体上的活动扣留（载体 -> 扣留标识集合），仅在出厂
            阶段拦截；存在任何活动扣留即禁止出厂。
        lineage: 目的地载体 -> 来源载体的谱系映射，缺省为同名直连。

    返回:
        允许的决策附带 ``shipment_id``、``stage``、``event_sequence``，
        重复扫描另带 ``replay=True`` 且不追加事件；拒绝的决策在 reasons 中
        逐条说明原因，且不写入任何事件（fail-closed）。
    """
    if stage not in STAGE_ORDER:
        raise ValueError("unknown transfer stage")

    payload = tuple(carriers)
    if len(set(payload)) != len(payload):
        return deny("duplicate_carrier_in_scan", shipment_id=shipment_id, stage=stage)

    state = transfer_state(log, shipment_id)
    current = state.stage
    evidence = {"shipment_id": shipment_id, "stage": stage}

    if current is not None:
        current_index = STAGE_ORDER.index(current)
        scan_index = STAGE_ORDER.index(stage)
        if scan_index < current_index or current == STAGE_UNLOAD or (scan_index == current_index and stage != STAGE_LOAD):
            # 回扫已完成阶段、整条链已结束，或重扫当前已完成阶段：只能幂等重放。
            # LOAD 阶段允许分批补载，由下面的装载分支单独处理。
            return _replay_or_changed(log, shipment_id, stage, payload, lineage)
        if scan_index > current_index + 1:
            expected = STAGE_ORDER[current_index + 1]
            return deny(f"stage_out_of_order:{stage}:{expected}", completed_stage=current, **evidence)

    if stage == STAGE_LOAD:
        if current not in (None, STAGE_LOAD):
            expected = STAGE_ORDER[STAGE_ORDER.index(current) + 1]
            return deny(f"stage_out_of_order:{stage}:{expected}", completed_stage=current, **evidence)
        if not payload:
            return deny("scan_without_carriers", **evidence)

        loaded_events = [
            event for event in log.read(_stream(shipment_id)) if event.kind == LOAD_SCANNED
        ]
        if any(frozenset(event.data["carriers"]) == frozenset(payload) for event in loaded_events):
            # 同一批载体原样重扫：返回首次装载事件，不重复记账。
            original = next(
                event for event in loaded_events if frozenset(event.data["carriers"]) == frozenset(payload)
            )
            return allow(
                event_sequence=original.sequence,
                replay=True,
                recorded_at=original.occurred_at,
                loaded_carriers=tuple(sorted(state.loaded_carriers)),
                **evidence,
            )
        overlap = state.loaded_carriers.intersection(payload)
        if overlap:
            return deny(
                *(f"carrier_already_loaded:{carrier}" for carrier in sorted(overlap)),
                loaded_carriers=tuple(sorted(state.loaded_carriers)),
                **evidence,
            )
        event_id = _scan_event_id(shipment_id, stage, payload)
        sequence = len(log.read(_stream(shipment_id)))
        event = log.append(
            _stream(shipment_id), sequence, event_id, at, LOAD_SCANNED, {"carriers": list(payload)}
        )
        return allow(
            event_sequence=event.sequence,
            replay=False,
            loaded_carriers=tuple(sorted(state.loaded_carriers.union(payload))),
            **evidence,
        )

    # 以下阶段都要求链已开始。
    if current is None:
        return deny(
            f"stage_out_of_order:{stage}:{STAGE_LOAD}", completed_stage=None, **evidence
        )

    if stage == STAGE_DEPART:
        if current != STAGE_LOAD:
            expected = STAGE_ORDER[STAGE_ORDER.index(current) + 1]
            return deny(f"stage_out_of_order:{stage}:{expected}", completed_stage=current, **evidence)
        hold_reasons = tuple(
            f"active_hold:{hold}"
            for carrier in sorted(state.loaded_carriers)
            for hold in sorted((active_holds or {}).get(carrier, frozenset()))
        )
        if hold_reasons:
            return deny(
                *hold_reasons,
                loaded_carriers=tuple(sorted(state.loaded_carriers)),
                **evidence,
            )
        event_id = _scan_event_id(shipment_id, stage, ())
        sequence = len(log.read(_stream(shipment_id)))
        event = log.append(_stream(shipment_id), sequence, event_id, at, DEPART_SCANNED, {})
        return allow(event_sequence=event.sequence, replay=False, **evidence)

    if stage == STAGE_ARRIVE:
        if current != STAGE_DEPART:
            expected = STAGE_ORDER[STAGE_ORDER.index(current) + 1]
            return deny(f"stage_out_of_order:{stage}:{expected}", completed_stage=current, **evidence)
        if not payload:
            return deny("scan_without_carriers", **evidence)
        scanned = frozenset(payload)
        if scanned != state.loaded_carriers:
            return deny(
                "arrival_carrier_mismatch",
                scanned_carriers=tuple(sorted(scanned)),
                expected_carriers=tuple(sorted(state.loaded_carriers)),
                missing_carriers=tuple(sorted(state.loaded_carriers - scanned)),
                unexpected_carriers=tuple(sorted(scanned - state.loaded_carriers)),
                **evidence,
            )
        event_id = _scan_event_id(shipment_id, stage, tuple(sorted(scanned)))
        sequence = len(log.read(_stream(shipment_id)))
        event = log.append(
            _stream(shipment_id), sequence, event_id, at, ARRIVE_SCANNED, {"carriers": list(payload)}
        )
        return allow(event_sequence=event.sequence, replay=False, **evidence)

    # stage == STAGE_UNLOAD
    if current != STAGE_ARRIVE:
        expected = STAGE_ORDER[STAGE_ORDER.index(current) + 1]
        return deny(f"stage_out_of_order:{stage}:{expected}", completed_stage=current, **evidence)
    if not payload:
        return deny("scan_without_carriers", **evidence)
    unloaded = frozenset(payload)
    lineage_values = set(lineage.values()) if lineage else set()
    unknown_origins = sorted(lineage_values - state.loaded_carriers)
    if unknown_origins:
        return deny(*(f"lineage_origin_not_loaded:{origin}" for origin in unknown_origins), **evidence)
    expected_destinations = set(lineage) if lineage else set(state.loaded_carriers)
    if unloaded != expected_destinations:
        return deny(
            "unload_carrier_mismatch",
            scanned_carriers=tuple(sorted(unloaded)),
            expected_carriers=tuple(sorted(expected_destinations)),
            missing_carriers=tuple(sorted(expected_destinations - unloaded)),
            unexpected_carriers=tuple(sorted(unloaded - expected_destinations)),
            **evidence,
        )
    links = tuple(sorted((dest, (lineage or {}).get(dest, dest)) for dest in unloaded))
    event_id = _scan_event_id(shipment_id, stage, tuple(sorted(unloaded)))
    sequence = len(log.read(_stream(shipment_id)))
    event = log.append(
        _stream(shipment_id),
        sequence,
        event_id,
        at,
        UNLOAD_SCANNED,
        {"carriers": sorted(unloaded), "lineage": [list(link) for link in links]},
    )
    return allow(event_sequence=event.sequence, replay=False, lineage_links=links, **evidence)


def transfer_links(state: TransferState) -> tuple[tuple[str, str], ...]:
    """返回卸载时记录的 ``(目的地载体, 来源载体)`` 谱系链接。"""
    return state.lineage_links
