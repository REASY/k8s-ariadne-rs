from __future__ import annotations

import re

from .ast_utils import _is_function_context, _iter_rule_contexts
from .text_utils import _looks_like_pattern_expression, _strip_string_literals


_UNSUPPORTED_FUNCTIONS = {
    "tobooleanlist",
    "tobooleanornull",
    "tofloatlist",
    "tofloatornull",
    "tointegerlist",
    "tointegerornull",
    "tostringlist",
    "isempty",
    "elementid",
    "nullif",
    "percentilecont",
    "percentiledisc",
    "stdev",
    "stdevp",
    "isnan",
    "cot",
    "degrees",
    "haversin",
    "radians",
    "normalize",
    "time",
    "shortestpath",
    "allshortestpaths",
}


def _find_compatibility_issues(
    text: str, tree: object | None, parser: object | None
) -> list[str]:
    stripped = _strip_string_literals(text)
    issues: list[str] = []

    if re.search(r":!", stripped):
        issues.append("NOT label expressions (:!Label) are not supported")

    if re.search(r"\bSHORTEST\b", stripped, re.IGNORECASE):
        issues.append("SHORTEST keyword is not supported; use Memgraph path syntax")

    if re.search(r"\bCOUNT\s*\{", stripped, re.IGNORECASE):
        issues.append("COUNT subqueries are not supported")

    if re.search(r"\bCOLLECT\s*\{", stripped, re.IGNORECASE):
        issues.append("COLLECT subqueries are not supported")

    if re.search(r"\bIS\s*::", stripped, re.IGNORECASE):
        issues.append("Type predicate 'IS ::' is not supported")

    if re.search(r"\b0o[0-7]+\b", stripped, re.IGNORECASE):
        issues.append("Octal integer literals (0o...) are not supported")

    if re.search(r"\b(NaN|Inf|Infinity)\b", stripped, re.IGNORECASE):
        issues.append("NaN/Inf/Infinity float literals are not supported")

    if re.search(r"(\]|-)\s*\{\s*\d", stripped):
        issues.append("Fixed-length patterns using '{n}' are not supported")

    if _case_when_has_multiple_values(stripped):
        issues.append(
            "CASE WHEN with multiple values (comma-separated) is not supported"
        )

    if tree is None or parser is None:
        return issues

    for rule_name, ctx, _ in _iter_rule_contexts(tree, parser):
        if _is_function_context(rule_name):
            func_name, args_text = _split_function_invocation(ctx.getText())
            func_name = func_name.lower()
            if func_name in _UNSUPPORTED_FUNCTIONS:
                issues.append(f"Function '{func_name}' is not supported")
                continue
            if func_name == "exists":
                if not _looks_like_pattern_expression(args_text):
                    issues.append(
                        "exists(n.property) is not supported; use IS NOT NULL"
                    )
                continue
            if _looks_like_pattern_expression(args_text):
                issues.append(
                    "Patterns in expressions are not supported (except EXISTS(pattern))"
                )
    return issues


def _case_when_has_multiple_values(stripped: str) -> bool:
    text = stripped
    upper = text.upper()
    i = 0
    depth_paren = 0
    depth_bracket = 0
    depth_brace = 0
    in_when = False
    comma_in_when = False
    while i < len(text):
        char = text[i]
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
            if upper.startswith("WHEN", i):
                in_when = True
                comma_in_when = False
                i += 4
                continue
            if in_when and upper.startswith("THEN", i):
                if comma_in_when:
                    return True
                in_when = False
                i += 4
                continue
            if in_when and char == ",":
                comma_in_when = True
        i += 1
    return False


def _split_function_invocation(text: str) -> tuple[str, str]:
    depth = 0
    for idx, char in enumerate(text):
        if char == "(":
            depth += 1
            name = text[:idx]
            args = text[idx + 1 : -1] if text.endswith(")") else text[idx + 1 :]
            return name.split(".")[-1], args
    return text, ""
