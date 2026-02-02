from __future__ import annotations

import logging

from ..cypher_ast import CypherAst, CypherParseError, parse_cypher
from ..graph_schema import GraphSchema
from .ast_utils import _is_pattern_context, _iter_rule_contexts
from .compatibility import _find_compatibility_issues
from .errors import (
    CypherCompatibilityError,
    CypherValidationError,
    SchemaValidationError,
)
from .extract import (
    _collect_variable_labels,
    _extract_rel_types,
    _format_snippet,
    _iter_relationships,
    _resolve_labels,
)
from .model import SchemaViolation
from .normalize import _normalize_exists_subqueries, _parse_with_fallback
from .schema_rules import _allowed_pairs, _direction_from_match, _is_allowed


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
