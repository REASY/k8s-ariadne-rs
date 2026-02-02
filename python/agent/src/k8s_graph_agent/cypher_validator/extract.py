from __future__ import annotations

from typing import Iterable

from .model import _NodeUse, _RelationshipUse
from .text_utils import _strip_string_literals


def _extract_labels(node_text: str) -> list[str]:
    trimmed = node_text.split("{", 1)[0].strip()
    if ":" not in trimmed:
        return []
    _, labels_part = trimmed.split(":", 1)
    labels = [label.strip("` ") for label in labels_part.split(":") if label.strip()]
    return labels


def _extract_rel_types(rel_text: str) -> list[str]:
    trimmed = rel_text.split("{", 1)[0].strip()
    if ":" not in trimmed:
        return []
    _, rel_part = trimmed.split(":", 1)
    rel_part = rel_part.split("*", 1)[0]
    rel_part = rel_part.strip()
    if not rel_part:
        return []
    parts = [piece.strip() for piece in rel_part.split("|") if piece.strip()]
    types: list[str] = []
    for part in parts:
        token = part.split()[0]
        if token:
            types.append(token)
    return types


def _iter_relationships(cypher: str) -> Iterable[_RelationshipUse]:
    text = _strip_string_literals(cypher)
    i = 0
    last_node: _NodeUse | None = None
    pending: _RelationshipUse | None = None
    while i < len(text):
        char = text[i]
        if char == "(":
            node_text, end = _consume_group(text, i, "(", ")")
            node = _parse_node(node_text)
            if pending is not None:
                yield _RelationshipUse(
                    left_node=pending.left_node,
                    right_node=node_text,
                    rel_text=pending.rel_text,
                    left_dir=pending.left_dir,
                    right_dir=pending.right_dir,
                    left_var=pending.left_var,
                    right_var=node.var,
                    left_labels=pending.left_labels,
                    right_labels=node.labels,
                )
                pending = None
            last_node = node
            i = end + 1
            continue
        if char == "[":
            rel_text, end = _consume_group(text, i, "[", "]")
            if last_node is not None:
                pending = _RelationshipUse(
                    left_node=last_node.text,
                    right_node="",
                    rel_text=rel_text,
                    left_dir=_read_left_dir(text, i),
                    right_dir=_read_right_dir(text, end),
                    left_var=last_node.var,
                    right_var=None,
                    left_labels=last_node.labels,
                    right_labels=(),
                )
            i = end + 1
            continue
        i += 1


def _consume_group(
    text: str, start: int, open_char: str, close_char: str
) -> tuple[str, int]:
    i = start + 1
    while i < len(text) and text[i] != close_char:
        i += 1
    return text[start + 1 : i], i


def _parse_node(node_text: str) -> _NodeUse:
    trimmed = node_text.split("{", 1)[0].strip()
    if not trimmed:
        return _NodeUse(node_text, None, ())
    parts = [part.strip() for part in trimmed.split(":")]
    var_part = parts[0].strip()
    if var_part:
        var = var_part.strip("` ")
        labels = parts[1:]
    else:
        var = None
        labels = parts[1:]
    cleaned = tuple(label.strip("` ") for label in labels if label.strip())
    return _NodeUse(node_text, var, cleaned)


def _iter_nodes(cypher: str) -> Iterable[_NodeUse]:
    text = _strip_string_literals(cypher)
    i = 0
    while i < len(text):
        char = text[i]
        if char == "(":
            node_text, end = _consume_group(text, i, "(", ")")
            yield _parse_node(node_text)
            i = end + 1
            continue
        i += 1


def _collect_variable_labels(pattern_texts: Iterable[str]) -> dict[str, frozenset[str]]:
    labels_by_var: dict[str, set[str]] = {}
    for pattern_text in pattern_texts:
        for node in _iter_nodes(pattern_text):
            if node.var and node.labels:
                labels_by_var.setdefault(node.var, set()).update(node.labels)
    return {var: frozenset(labels) for var, labels in labels_by_var.items()}


def _resolve_labels(
    explicit: tuple[str, ...],
    var: str | None,
    variable_labels: dict[str, frozenset[str]],
) -> tuple[str, ...]:
    if explicit:
        return explicit
    if var and var in variable_labels:
        return tuple(variable_labels[var])
    return ()


def _read_left_dir(text: str, rel_start: int) -> str:
    i = rel_start - 1
    while i >= 0 and text[i].isspace():
        i -= 1
    if i >= 1 and text[i] == "-" and text[i - 1] == "<":
        return "<-"
    if i >= 0 and text[i] == "-":
        return "-"
    return "-"


def _read_right_dir(text: str, rel_end: int) -> str:
    i = rel_end + 1
    while i < len(text) and text[i].isspace():
        i += 1
    if i + 1 < len(text) and text[i] == "-" and text[i + 1] == ">":
        return "->"
    if i < len(text) and text[i] == "-":
        return "-"
    return "-"


def _format_snippet(rel_use: _RelationshipUse) -> str:
    return (
        f"({rel_use.left_node}){rel_use.left_dir}[{rel_use.rel_text}]"
        f"{rel_use.right_dir}({rel_use.right_node})"
    )
