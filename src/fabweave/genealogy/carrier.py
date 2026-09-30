from __future__ import annotations

from collections import defaultdict, deque


def ancestry(parent_of: dict[str, str | None], carrier: str) -> list[str]:
    out: list[str] = []
    seen = {carrier}
    current = carrier
    while current in parent_of and parent_of[current] is not None:
        parent = parent_of[current]
        if parent in seen:
            raise ValueError("cycle detected")
        seen.add(parent)
        out.append(parent)
        current = parent
    return out


class GenealogyGraph:
    def __init__(self, parent_of: dict[str, str | None]) -> None:
        self._parents = dict(parent_of)
        self._children: dict[str, set[str]] = defaultdict(set)
        for child, parent in self._parents.items():
            if parent is not None:
                self._children[parent].add(child)
        for node in self._parents:
            ancestry(self._parents, node)

    def parents(self) -> dict[str, str | None]:
        """返回 ``子载体 -> 父载体`` 映射的只读快照（根映射到 ``None``）。"""
        return dict(self._parents)

    def related(self, start: str, direction: str, max_depth: int) -> tuple[str, ...]:
        if max_depth < 0 or direction not in {"upstream", "downstream"}:
            raise ValueError("invalid traversal request")
        found: set[str] = set()
        queue = deque([(start, 0)])
        while queue:
            node, depth = queue.popleft()
            if depth == max_depth:
                continue
            neighbors = (
                [self._parents.get(node)]
                if direction == "upstream"
                else sorted(self._children.get(node, set()))
            )
            for neighbor in neighbors:
                if neighbor is None or neighbor == start or neighbor in found:
                    continue
                found.add(neighbor)
                queue.append((neighbor, depth + 1))
        return tuple(sorted(found))
