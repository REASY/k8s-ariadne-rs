from __future__ import annotations

import re

from .text_utils import _is_word_boundary, _looks_like_pattern_expression
from ..cypher_ast import CypherAst, CypherParseError, parse_cypher


_CLAUSE_START_RE = re.compile(
    r"\b(OPTIONAL\s+MATCH|MATCH|UNWIND|CALL|CREATE|MERGE|SET|DELETE|DETACH|REMOVE|RETURN)\b",
    re.IGNORECASE,
)


def _parse_with_fallback(text: str) -> list[CypherAst]:
    segments = _split_top_level_keyword(text, "WITH")
    asts: list[CypherAst] = []
    for segment in segments:
        trimmed = _strip_to_first_clause(segment)
        if not trimmed:
            continue
        candidate = _ensure_return_clause(trimmed)
        try:
            asts.append(parse_cypher(candidate))
        except CypherParseError:
            continue
    return asts


def _normalize_exists_subqueries(text: str) -> str:
    upper = text.upper()
    result: list[str] = []
    last = 0
    i = 0
    in_string = False
    in_backtick = False
    while i < len(text):
        char = text[i]
        if in_string:
            if char == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            if char == "'":
                in_string = False
            i += 1
            continue
        if in_backtick:
            if char == "`":
                in_backtick = False
            i += 1
            continue
        if char == "'":
            in_string = True
            i += 1
            continue
        if char == "`":
            in_backtick = True
            i += 1
            continue
        if upper.startswith("EXISTS", i) and _is_word_boundary(text, i, i + 6):
            j = i + 6
            while j < len(text) and text[j].isspace():
                j += 1
            if j < len(text) and text[j] == "{":
                end = _find_matching_brace(text, j)
                if end is None:
                    break
                body = text[j + 1 : end]
                normalized_body = _normalize_exists_subqueries(body)
                if not _subquery_has_top_level_return(normalized_body):
                    normalized_body = normalized_body.rstrip()
                    if normalized_body and not normalized_body.endswith(" "):
                        normalized_body += " "
                    normalized_body += "RETURN 1"
                result.append(text[last:i])
                result.append(text[i:j])
                result.append("{")
                result.append(normalized_body)
                result.append("}")
                i = end + 1
                last = i
                continue
            if j < len(text) and text[j] == "(":
                end = _find_matching_paren(text, j)
                if end is None:
                    break
                body = text[j + 1 : end]
                body_stripped = body.strip()
                if _looks_like_pattern_expression(body_stripped):
                    replacement = "EXISTS { MATCH "
                    replacement += body_stripped
                    replacement += " RETURN 1 }"
                    result.append(text[last:i])
                    result.append(replacement)
                    i = end + 1
                    last = i
                    continue
        i += 1
    if last < len(text):
        result.append(text[last:])
    return "".join(result)


def _find_matching_brace(text: str, start: int) -> int | None:
    depth = 0
    i = start
    in_string = False
    in_backtick = False
    while i < len(text):
        char = text[i]
        if in_string:
            if char == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            if char == "'":
                in_string = False
            i += 1
            continue
        if in_backtick:
            if char == "`":
                in_backtick = False
            i += 1
            continue
        if char == "'":
            in_string = True
            i += 1
            continue
        if char == "`":
            in_backtick = True
            i += 1
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _find_matching_paren(text: str, start: int) -> int | None:
    depth = 0
    i = start
    in_string = False
    in_backtick = False
    while i < len(text):
        char = text[i]
        if in_string:
            if char == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            if char == "'":
                in_string = False
            i += 1
            continue
        if in_backtick:
            if char == "`":
                in_backtick = False
            i += 1
            continue
        if char == "'":
            in_string = True
            i += 1
            continue
        if char == "`":
            in_backtick = True
            i += 1
            continue
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    return None


def _subquery_has_top_level_return(text: str) -> bool:
    upper = text.upper()
    i = 0
    depth_paren = 0
    depth_bracket = 0
    depth_brace = 0
    in_string = False
    in_backtick = False
    while i < len(text):
        char = text[i]
        if in_string:
            if char == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            if char == "'":
                in_string = False
            i += 1
            continue
        if in_backtick:
            if char == "`":
                in_backtick = False
            i += 1
            continue
        if char == "'":
            in_string = True
            i += 1
            continue
        if char == "`":
            in_backtick = True
            i += 1
            continue
        if char == "(":
            depth_paren += 1
        elif char == ")":
            depth_paren = max(0, depth_paren - 1)
        elif char == "[":
            depth_bracket += 1
        elif char == "]":
            depth_bracket = max(0, depth_bracket - 1)
        elif char == "{":
            depth_brace += 1
        elif char == "}":
            depth_brace = max(0, depth_brace - 1)

        if depth_paren == 0 and depth_bracket == 0 and depth_brace == 0:
            if upper.startswith("RETURN", i) and _is_word_boundary(text, i, i + 6):
                return True
        i += 1
    return False


def _split_top_level_keyword(text: str, keyword: str) -> list[str]:
    upper = text.upper()
    target = keyword.upper()
    segments: list[str] = []
    start = 0
    i = 0
    depth_paren = 0
    depth_bracket = 0
    depth_brace = 0
    in_string = False
    in_backtick = False
    while i < len(text):
        char = text[i]
        if in_string:
            if char == "'" and i + 1 < len(text) and text[i + 1] == "'":
                i += 2
                continue
            if char == "'":
                in_string = False
            i += 1
            continue
        if in_backtick:
            if char == "`":
                in_backtick = False
            i += 1
            continue
        if char == "'":
            in_string = True
            i += 1
            continue
        if char == "`":
            in_backtick = True
            i += 1
            continue
        if char == "(":
            depth_paren += 1
        elif char == ")":
            depth_paren = max(0, depth_paren - 1)
        elif char == "[":
            depth_bracket += 1
        elif char == "]":
            depth_bracket = max(0, depth_bracket - 1)
        elif char == "{":
            depth_brace += 1
        elif char == "}":
            depth_brace = max(0, depth_brace - 1)

        if depth_paren == 0 and depth_bracket == 0 and depth_brace == 0:
            if upper.startswith(target, i) and _is_word_boundary(
                text, i, i + len(target)
            ):
                segments.append(text[start:i])
                i += len(target)
                start = i
                continue
        i += 1
    segments.append(text[start:])
    return segments


def _strip_to_first_clause(text: str) -> str:
    match = _CLAUSE_START_RE.search(text)
    if not match:
        return ""
    return text[match.start() :].strip()


def _ensure_return_clause(text: str) -> str:
    trimmed = text.strip().rstrip(";")
    if re.search(r"\bRETURN\b", trimmed, re.IGNORECASE):
        return trimmed
    if re.search(
        r"\b(CREATE|MERGE|SET|DELETE|DETACH|REMOVE)\b", trimmed, re.IGNORECASE
    ):
        return trimmed
    return f"{trimmed} RETURN 1"
