from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class _RelationshipUse:
    left_node: str
    right_node: str
    rel_text: str
    left_dir: str
    right_dir: str
    left_var: str | None
    right_var: str | None
    left_labels: tuple[str, ...]
    right_labels: tuple[str, ...]


@dataclass(frozen=True)
class _NodeUse:
    text: str
    var: str | None
    labels: tuple[str, ...]


@dataclass(frozen=True)
class SchemaViolation:
    rel_type: str
    left_labels: tuple[str, ...]
    right_labels: tuple[str, ...]
    direction: str
    snippet: str
    rule_path: str
    allowed_pairs: tuple[tuple[str, str], ...]
