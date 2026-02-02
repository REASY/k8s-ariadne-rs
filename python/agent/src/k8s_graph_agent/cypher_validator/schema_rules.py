from __future__ import annotations

from ..graph_schema import GraphSchema


def _direction_from_match(left_dir: str, right_dir: str) -> str:
    if left_dir == "<-" and right_dir == "->":
        return "both"
    if left_dir == "<-":
        return "right_to_left"
    if right_dir == "->":
        return "left_to_right"
    return "undirected"


def _is_allowed(
    schema: GraphSchema,
    rel_types: list[str],
    left_labels: tuple[str, ...],
    right_labels: tuple[str, ...],
    direction: str,
) -> bool:
    for rel_type in rel_types:
        if direction in {"left_to_right", "both", "undirected"}:
            for left in left_labels:
                for right in right_labels:
                    if schema.allows(rel_type, left, right):
                        return True
        if direction in {"right_to_left", "both", "undirected"}:
            for left in left_labels:
                for right in right_labels:
                    if schema.allows(rel_type, right, left):
                        return True
    return False


def _allowed_pairs(
    schema: GraphSchema, rel_types: list[str]
) -> tuple[tuple[str, str], ...]:
    pairs: list[tuple[str, str]] = []
    for rel_type in rel_types:
        for src, dst in schema.relationships.get(rel_type, frozenset()):
            pair = (src, dst)
            if pair not in pairs:
                pairs.append(pair)
    return tuple(pairs)
