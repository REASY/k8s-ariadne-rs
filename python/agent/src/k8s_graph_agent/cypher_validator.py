from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from antlr4 import ParserRuleContext

from .cypher_ast import CypherParseError, parse_cypher
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


@dataclass(frozen=True)
class SchemaViolation:
    rel_type: str
    left_labels: tuple[str, ...]
    right_labels: tuple[str, ...]
    direction: str
    snippet: str


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

    @classmethod
    def for_default_schema(cls) -> "CypherSchemaValidator":
        return cls(GraphSchema.load_default())

    def validate(self, cypher: str) -> None:
        try:
            ast = parse_cypher(cypher)
        except CypherParseError as exc:
            raise CypherValidationError(str(exc)) from exc

        compatibility_issues = _find_compatibility_issues(ast.text, ast.tree, ast.parser)
        if compatibility_issues:
            raise CypherCompatibilityError(compatibility_issues)

        violations: list[SchemaViolation] = []
        for rule_name, ctx in _iter_rule_contexts(ast.tree, ast.parser):
            if _is_pattern_context(rule_name):
                for rel_use in _iter_relationships(ctx.getText()):
                    rel_types = _extract_rel_types(rel_use.rel_text)
                    if not rel_types:
                        continue
                    left_labels = tuple(_extract_labels(rel_use.left_node))
                    right_labels = tuple(_extract_labels(rel_use.right_node))
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
                        )
                    )
        if violations:
            raise SchemaValidationError(violations)


def _find_compatibility_issues(text: str, tree: object, parser: object) -> list[str]:
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
        issues.append("CASE WHEN with multiple values (comma-separated) is not supported")

    for rule_name, ctx in _iter_rule_contexts(tree, parser):
        if _is_function_context(rule_name):
            func_name, args_text = _split_function_invocation(ctx.getText())
            func_name = func_name.lower()
            if func_name in _UNSUPPORTED_FUNCTIONS:
                issues.append(f"Function '{func_name}' is not supported")
                continue
            if func_name == "exists":
                if not _looks_like_pattern_expression(args_text):
                    issues.append("exists(n.property) is not supported; use IS NOT NULL")
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


def _iter_rule_contexts(tree: object, parser: object) -> Iterable[tuple[str, ParserRuleContext]]:
    stack = [tree]
    while stack:
        node = stack.pop()
        if isinstance(node, ParserRuleContext):
            rule_name = parser.ruleNames[node.getRuleIndex()]
            yield rule_name, node
            if node.children:
                stack.extend(reversed(node.children))


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
    last_node: str | None = None
    pending: _RelationshipUse | None = None
    while i < len(text):
        char = text[i]
        if char == "(":
            node_text, end = _consume_group(text, i, "(", ")")
            if pending is not None:
                yield _RelationshipUse(
                    left_node=pending.left_node,
                    right_node=node_text,
                    rel_text=pending.rel_text,
                    left_dir=pending.left_dir,
                    right_dir=pending.right_dir,
                )
                pending = None
            last_node = node_text
            i = end + 1
            continue
        if char == "[":
            rel_text, end = _consume_group(text, i, "[", "]")
            if last_node is not None:
                pending = _RelationshipUse(
                    left_node=last_node,
                    right_node="",
                    rel_text=rel_text,
                    left_dir=_read_left_dir(text, i),
                    right_dir=_read_right_dir(text, end),
                )
            i = end + 1
            continue
        i += 1


def _consume_group(text: str, start: int, open_char: str, close_char: str) -> tuple[str, int]:
    i = start + 1
    while i < len(text) and text[i] != close_char:
        i += 1
    return text[start + 1 : i], i


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
        lines.append(
            "- %s %s %s via %s (%s)"
            % (
                ",".join(violation.left_labels),
                "->" if violation.direction == "left_to_right" else "<->",
                ",".join(violation.right_labels),
                violation.rel_type,
                violation.snippet,
            )
        )
    return "\n".join(lines)
