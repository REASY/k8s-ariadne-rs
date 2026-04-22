from __future__ import annotations

import re
from typing import Iterable


_NOT_EXISTS_RE = re.compile(
    r"NOT\s+EXISTS\s*\{\s*MATCH\s+(?P<body>.*?)\s*\}",
    re.IGNORECASE | re.DOTALL,
)
_UNWIND_RE = re.compile(
    r"\bUNWIND\s+(?P<expr>.+?)\s+AS\s+(?P<alias>[A-Za-z_]\w*)",
    re.IGNORECASE | re.DOTALL,
)
_CLAUSE_RE = re.compile(
    r"\b(?P<kind>OPTIONAL MATCH|MATCH)\b\s+(?P<body>.*?)(?=(?:\bOPTIONAL MATCH\b|\bMATCH\b|\bUNWIND\b|\bWITH\b|\bRETURN\b|\bCALL\b|\bORDER\s+BY\b|\bLIMIT\b|$))",
    re.IGNORECASE | re.DOTALL,
)
_NODE_RE = re.compile(
    r"\(\s*(?P<alias>[A-Za-z_]\w*)?\s*(?::\s*(?P<label>[A-Za-z_]\w*))?[^)]*\)"
)
_REL_RE = re.compile(
    r"(?P<dir><-\s*\[:\s*(?P<left_type>[A-Za-z_]\w*)\s*\]\s*-|-\s*\[:\s*(?P<right_type>[A-Za-z_]\w*)\s*\]\s*->)"
)
_ALIASED_PROP_RE = re.compile(r"(?P<alias>[A-Za-z_]\w*)(?P<path>(?:\[[^\]]+\])+)")


def derive_traversal_plan(cypher: str) -> str:
    alias_labels = _collect_alias_labels(cypher)
    lines: list[str] = []

    for body in _NOT_EXISTS_RE.findall(cypher):
        for plan in _parse_patterns(body, alias_labels):
            if plan:
                lines.append(f"EXCLUDE {plan}")

    remaining = _NOT_EXISTS_RE.sub("", cypher)

    for expr, alias in _UNWIND_RE.findall(remaining):
        normalized = _normalize_expression(expr, alias_labels)
        lines.append(f"UNWIND {normalized} AS {alias}")

    for kind, body in _CLAUSE_RE.findall(remaining):
        prefix = "OPTIONAL" if kind.upper().startswith("OPTIONAL") else "MATCH"
        for plan in _parse_patterns(body, alias_labels):
            if not plan:
                continue
            if prefix == "MATCH":
                lines.append(plan)
            else:
                lines.append(f"OPTIONAL {plan}")

    deduped: list[str] = []
    seen: set[str] = set()
    for line in lines:
        if line in seen:
            continue
        seen.add(line)
        deduped.append(line)
    return "\n".join(deduped)


def _collect_alias_labels(cypher: str) -> dict[str, str]:
    alias_labels: dict[str, str] = {}
    for match in _NODE_RE.finditer(cypher):
        alias = match.group("alias")
        label = match.group("label")
        if alias and label:
            alias_labels[alias] = label
    return alias_labels


def _parse_patterns(body: str, alias_labels: dict[str, str]) -> list[str]:
    patterns = _split_top_level_patterns(body)
    rendered: list[str] = []
    for pattern in patterns:
        plan = _render_pattern(pattern.strip(), alias_labels)
        if plan:
            rendered.append(plan)
    return rendered


def _split_top_level_patterns(body: str) -> list[str]:
    parts: list[str] = []
    current: list[str] = []
    paren = bracket = brace = 0
    for char in body:
        if char == "(":
            paren += 1
        elif char == ")":
            paren = max(0, paren - 1)
        elif char == "[":
            bracket += 1
        elif char == "]":
            bracket = max(0, bracket - 1)
        elif char == "{":
            brace += 1
        elif char == "}":
            brace = max(0, brace - 1)
        elif char == "," and paren == 0 and bracket == 0 and brace == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
            continue
        current.append(char)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _render_pattern(pattern: str, alias_labels: dict[str, str]) -> str | None:
    node_matches = list(_NODE_RE.finditer(pattern))
    rel_matches = list(_REL_RE.finditer(pattern))
    if not node_matches:
        return None
    if not rel_matches:
        alias = node_matches[0].group("alias")
        label = node_matches[0].group("label") or (alias_labels.get(alias) if alias else None)
        return label

    pieces: list[str] = []
    for index, rel_match in enumerate(rel_matches):
        left_node = node_matches[index]
        right_node = node_matches[index + 1]
        left_label = _node_label(left_node, alias_labels)
        right_label = _node_label(right_node, alias_labels)
        rel_type = rel_match.group("left_type") or rel_match.group("right_type")
        if not left_label or not right_label or not rel_type:
            continue
        if rel_match.group("left_type"):
            segment = f"{right_label} -[:{rel_type}]-> {left_label}"
        else:
            segment = f"{left_label} -[:{rel_type}]-> {right_label}"
        pieces.append(segment)
    return "; ".join(pieces) if pieces else None


def _node_label(node_match: re.Match[str], alias_labels: dict[str, str]) -> str | None:
    alias = node_match.group("alias")
    label = node_match.group("label")
    if label:
        return label
    if alias:
        return alias_labels.get(alias)
    return None


def _normalize_expression(expr: str, alias_labels: dict[str, str]) -> str:
    compact = " ".join(expr.strip().split())

    def replace(match: re.Match[str]) -> str:
        alias = match.group("alias")
        path = match.group("path")
        label = alias_labels.get(alias, alias)
        dot_path = _bracket_path_to_dot(path)
        return f"{label}.{dot_path}" if dot_path else label

    compact = _ALIASED_PROP_RE.sub(replace, compact)
    return compact


def _bracket_path_to_dot(path: str) -> str:
    parts: list[str] = []
    for key in re.findall(r"\[\s*'([^']+)'\s*\]", path):
        parts.append(key)
    return ".".join(parts)


def render_sample_traversal_plans(
    cyphers: Iterable[str],
) -> list[str]:
    return [derive_traversal_plan(cypher) for cypher in cyphers]
