from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Iterable

from antlr4 import ParserRuleContext

from .cypher_ast import CypherAst, CypherParseError, parse_cypher
from .graph_schema import GraphSchema


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


class CypherValidationError(ValueError):
    pass


class SchemaValidationError(CypherValidationError):
    def __init__(self, violations: Iterable[SchemaViolation]) -> None:
        self.violations = list(violations)
        message = _format_violations(self.violations)
        super().__init__(message)


class CypherCompatibilityError(CypherValidationError):
    def __init__(self, issues: Iterable[str]) -> None:
        self.issues = list(issues)
        message = "Cypher uses constructs not supported by Memgraph:\n" + "\n".join(
            f"- {issue}" for issue in self.issues
        )
        super().__init__(message)


class CypherSchemaValidator:
    def __init__(self, schema: GraphSchema) -> None:
        self._schema = schema
        self._logger = logging.getLogger(__name__)

    @classmethod
    def for_default_schema(cls) -> "CypherSchemaValidator":
        return cls(GraphSchema.load_default())

    def validate(self, cypher: str) -> None:
        used_fallback = False
        asts: list[CypherAst] = []
        normalized: str | None = None
        try:
            asts = [parse_cypher(cypher)]
        except CypherParseError as exc:
            normalized = _normalize_exists_subqueries(cypher)
            try:
                asts = [parse_cypher(normalized)]
            except CypherParseError:
                asts = _parse_with_fallback(normalized)
                if not asts:
                    raise CypherValidationError(str(exc)) from exc
                used_fallback = True
                self._logger.warning(
                    "Cypher parse failed; using fallback segmentation for schema validation"
                )

        compatibility_issues = _find_compatibility_issues(
            cypher,
            asts[0].tree if not used_fallback else None,
            asts[0].parser if not used_fallback else None,
        )
        if used_fallback and compatibility_issues:
            self._logger.warning(
                "Compatibility checks are partial due to fallback parsing"
            )
        if compatibility_issues:
            raise CypherCompatibilityError(compatibility_issues)

        pattern_parts: list[tuple[str, str]] = []
        for ast in asts:
            for rule_name, ctx, rule_path in _iter_rule_contexts(ast.tree, ast.parser):
                if _is_pattern_context(rule_name):
                    pattern_parts.append(("/".join(rule_path), ctx.getText()))

        variable_labels = _collect_variable_labels(
            [pattern_text for _, pattern_text in pattern_parts]
        )

        violations: list[SchemaViolation] = []
        for rule_path, pattern_text in pattern_parts:
            for rel_use in _iter_relationships(pattern_text):
                rel_types = _extract_rel_types(rel_use.rel_text)
                if not rel_types:
                    continue
                left_labels = _resolve_labels(
                    rel_use.left_labels, rel_use.left_var, variable_labels
                )
                right_labels = _resolve_labels(
                    rel_use.right_labels, rel_use.right_var, variable_labels
                )
                if not left_labels or not right_labels:
                    continue
                direction = _direction_from_match(rel_use.left_dir, rel_use.right_dir)
                if _is_allowed(
                    self._schema, rel_types, left_labels, right_labels, direction
                ):
                    continue
                violations.append(
                    SchemaViolation(
                        rel_type="|".join(rel_types),
                        left_labels=left_labels,
                        right_labels=right_labels,
                        direction=direction,
                        snippet=_format_snippet(rel_use),
                        rule_path=rule_path,
                        allowed_pairs=_allowed_pairs(self._schema, rel_types),
                    )
                )
        if violations:
            raise SchemaValidationError(violations)


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


def _looks_like_pattern_expression(text: str) -> bool:
    return any(token in text for token in ("-[:", "<-[", "]-", "->", "<-", ")-", "-("))


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


def _is_word_boundary(text: str, start: int, end: int) -> bool:
    if start > 0 and (text[start - 1].isalnum() or text[start - 1] == "_"):
        return False
    if end < len(text) and (text[end].isalnum() or text[end] == "_"):
        return False
    return True


def _iter_rule_contexts(
    tree: object, parser: object
) -> Iterable[tuple[str, ParserRuleContext, tuple[str, ...]]]:
    stack: list[tuple[object, tuple[str, ...]]] = [(tree, ())]
    while stack:
        node, path = stack.pop()
        if isinstance(node, ParserRuleContext):
            rule_name = parser.ruleNames[node.getRuleIndex()]
            next_path = path + (rule_name,)
            yield rule_name, node, next_path
            if node.children:
                stack.extend((child, next_path) for child in reversed(node.children))


def _is_pattern_context(rule_name: str) -> bool:
    name = rule_name.lower()
    return name.endswith("patternpart") or name.endswith("patternelement")


def _is_function_context(rule_name: str) -> bool:
    return rule_name.lower().endswith("functioninvocation")


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


def _strip_string_literals(text: str) -> str:
    result: list[str] = []
    in_string = False
    escape = False
    for char in text:
        if in_string:
            if escape:
                escape = False
                result.append(" ")
                continue
            if char == "\\":
                escape = True
                result.append(" ")
                continue
            if char == "'":
                in_string = False
            result.append(" ")
            continue
        if char == "'":
            in_string = True
            result.append(" ")
            continue
        result.append(char)
    return "".join(result)


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


def _format_violations(violations: list[SchemaViolation]) -> str:
    lines = ["Cypher schema validation failed:"]
    for violation in violations:
        if violation.direction == "left_to_right":
            arrow = "->"
        elif violation.direction == "right_to_left":
            arrow = "<-"
        elif violation.direction == "both":
            arrow = "<->"
        else:
            arrow = "-"
        allowed = _format_allowed_pairs(violation.allowed_pairs)
        lines.append(
            "- Invalid relationship: %s %s %s via %s. Allowed: %s. Pattern: %s [rule=%s]"
            % (
                ",".join(violation.left_labels),
                arrow,
                ",".join(violation.right_labels),
                violation.rel_type,
                allowed,
                violation.snippet,
                violation.rule_path,
            )
        )
        lines.append(
            "  Hint: %s is only allowed as %s. Check direction and node labels."
            % (violation.rel_type, allowed)
        )
    return "\n".join(lines)


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


def _format_allowed_pairs(pairs: tuple[tuple[str, str], ...]) -> str:
    if not pairs:
        return "none"
    return "; ".join(f"{src} -> {dst}" for src, dst in pairs)
