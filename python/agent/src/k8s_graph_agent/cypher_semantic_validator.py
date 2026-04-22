"""Semantic validation of LLM-generated Cypher against the graph schema.

Walks the ANTLR Cypher AST and checks:
1. Variable scope — catches references to variables dropped by WITH
2. Relationship legality — catches invalid (src)-[:Rel]->(dst) triples
3. Node label validity — catches unknown entity types
4. Provides structured error messages suitable for LLM retry prompts
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from antlr4 import ParserRuleContext

from .cypher_ast import CypherAst, parse_cypher, CypherParseError
from .graph_schema import GraphSchema


@dataclass(frozen=True)
class SemanticError:
    line: int | None
    message: str
    suggestion: str | None = None

    def __str__(self) -> str:
        loc = f"Line {self.line}: " if self.line else ""
        sug = f" Suggestion: {self.suggestion}" if self.suggestion else ""
        return f"{loc}{self.message}{sug}"


@dataclass
class ValidationResult:
    errors: list[SemanticError] = field(default_factory=list)

    @property
    def valid(self) -> bool:
        return len(self.errors) == 0

    def format_for_retry(self) -> str:
        if not self.errors:
            return ""
        lines = ["Cypher validation errors:"]
        for i, err in enumerate(self.errors, 1):
            lines.append(f"  {i}. {err}")
        return "\n".join(lines)


class CypherSemanticValidator:
    """Validates Cypher AST against a GraphSchema."""

    def __init__(self, schema: GraphSchema) -> None:
        self._schema = schema
        self._all_labels = self._collect_labels()

    def validate(self, cypher: str) -> ValidationResult:
        result = ValidationResult()
        try:
            ast = parse_cypher(cypher)
        except CypherParseError as exc:
            result.errors.append(SemanticError(
                line=None,
                message=f"Syntax error: {exc}",
            ))
            return result

        clauses = _extract_clauses(ast)
        scope = _VariableScope()

        for clause in clauses:
            if clause.kind == "match":
                self._validate_match(clause, scope, result)
            elif clause.kind == "with":
                self._validate_with_projection(clause, scope, result)
            elif clause.kind == "return":
                self._validate_return(clause, scope, result)
            elif clause.kind == "unwind":
                self._validate_unwind(clause, scope, result)
            elif clause.kind in ("where", "order_by"):
                self._validate_expression_scope(clause, scope, result)

        return result

    def _validate_match(
        self, clause: _Clause, scope: _VariableScope, result: ValidationResult
    ) -> None:
        for pattern in clause.patterns:
            # Validate node labels and register variables
            for node in pattern.nodes:
                if node.label and node.label not in self._all_labels:
                    result.errors.append(SemanticError(
                        line=node.line,
                        message=f"Unknown node label '{node.label}'.",
                        suggestion=f"Valid labels: {', '.join(sorted(self._all_labels)[:10])}...",
                    ))
                if node.variable:
                    scope.bind(node.variable, node.label)

            # Validate relationships
            for rel in pattern.relationships:
                if not rel.rel_type:
                    continue
                src_label = rel.src_label or scope.label_of(rel.src_var)
                dst_label = rel.dst_label or scope.label_of(rel.dst_var)
                if src_label and dst_label and rel.rel_type:
                    if rel.direction == "right":
                        if not self._schema.allows(rel.rel_type, src_label, dst_label):
                            valid_targets = self._valid_targets(rel.rel_type, src_label)
                            result.errors.append(SemanticError(
                                line=rel.line,
                                message=(
                                    f"Relationship {src_label} -[:{rel.rel_type}]-> {dst_label} "
                                    f"is not valid in the schema."
                                ),
                                suggestion=(
                                    f"Valid targets for {src_label} -[:{rel.rel_type}]->: "
                                    f"{', '.join(valid_targets) if valid_targets else 'none'}"
                                ) if valid_targets is not None else None,
                            ))
                    elif rel.direction == "left":
                        if not self._schema.allows(rel.rel_type, dst_label, src_label):
                            valid_sources = self._valid_sources(rel.rel_type, dst_label)
                            result.errors.append(SemanticError(
                                line=rel.line,
                                message=(
                                    f"Relationship {src_label} <-[:{rel.rel_type}]- {dst_label} "
                                    f"is not valid in the schema."
                                ),
                                suggestion=(
                                    f"Valid sources for <-[:{rel.rel_type}]- {dst_label}: "
                                    f"{', '.join(valid_sources) if valid_sources else 'none'}"
                                ) if valid_sources is not None else None,
                            ))

    def _validate_with_projection(
        self, clause: _Clause, scope: _VariableScope, result: ValidationResult
    ) -> None:
        # First validate that all referenced variables exist in scope
        for item in clause.projection_items:
            for var in _extract_variable_refs(item.expression):
                if var not in scope.variables:
                    result.errors.append(SemanticError(
                        line=clause.line,
                        message=f"Variable '{var}' referenced in WITH is not in scope.",
                    ))

        # Build the new scope: aliased names replace originals
        new_vars: dict[str, str | None] = {}
        for item in clause.projection_items:
            if item.alias:
                # WITH expr AS alias — alias is the new variable name.
                # Try to carry forward the label from the original variable.
                refs = _extract_variable_refs(item.expression)
                label = scope.label_of(refs[0]) if refs else None
                new_vars[item.alias] = label
            else:
                # WITH var — keep the original variable name and label
                var_name = item.expression.strip()
                new_vars[var_name] = scope.label_of(var_name)

        # Also handle WITH * (all variables projected)
        if any(item.expression.strip() == "*" for item in clause.projection_items):
            new_vars.update(scope.variables)

        # Project scope — drop variables not in the new set
        scope.project_to_new(new_vars)

    def _validate_return(
        self, clause: _Clause, scope: _VariableScope, result: ValidationResult
    ) -> None:
        for item in clause.projection_items:
            for var in _extract_variable_refs(item.expression):
                if var not in scope.variables:
                    dropped_at = scope.dropped_at(var)
                    suggestion = None
                    if dropped_at:
                        suggestion = (
                            f"'{var}' was dropped by WITH. "
                            f"Add '{var}' to the WITH clause to keep it in scope."
                        )
                    result.errors.append(SemanticError(
                        line=clause.line,
                        message=f"Variable '{var}' is not in scope in RETURN.",
                        suggestion=suggestion,
                    ))
            # Add RETURN aliases to scope so ORDER BY can reference them
            if item.alias:
                scope.bind(item.alias, None)

    def _validate_unwind(
        self, clause: _Clause, scope: _VariableScope, result: ValidationResult
    ) -> None:
        # Validate the source expression references are in scope
        if clause.unwind_source:
            for var in _extract_variable_refs(clause.unwind_source):
                if var not in scope.variables:
                    result.errors.append(SemanticError(
                        line=clause.line,
                        message=f"Variable '{var}' referenced in UNWIND source is not in scope.",
                    ))
        if clause.unwind_alias:
            scope.bind(clause.unwind_alias, None)

    def _validate_expression_scope(
        self, clause: _Clause, scope: _VariableScope, result: ValidationResult
    ) -> None:
        """Validate that all variable references in WHERE or ORDER BY are in scope."""
        clause_name = "WHERE" if clause.kind == "where" else "ORDER BY"
        for expr in clause.expressions:
            for var in _extract_variable_refs(expr):
                if var not in scope.variables:
                    dropped = scope.dropped_at(var)
                    suggestion = None
                    if dropped:
                        suggestion = (
                            f"'{var}' was dropped by WITH. "
                            f"Add '{var}' to the WITH clause to keep it in scope."
                        )
                    result.errors.append(SemanticError(
                        line=clause.line,
                        message=f"Variable '{var}' is not in scope in {clause_name}.",
                        suggestion=suggestion,
                    ))

    def _collect_labels(self) -> set[str]:
        labels: set[str] = set()
        for pairs in self._schema.relationships.values():
            for src, dst in pairs:
                labels.add(src)
                labels.add(dst)
        return labels

    def _valid_targets(self, rel_type: str, src_label: str) -> list[str] | None:
        pairs = self._schema.relationships.get(rel_type)
        if pairs is None:
            return None
        return sorted(dst for s, dst in pairs if s == src_label)

    def _valid_sources(self, rel_type: str, dst_label: str) -> list[str] | None:
        pairs = self._schema.relationships.get(rel_type)
        if pairs is None:
            return None
        return sorted(src for src, d in pairs if d == dst_label)


# ---------------------------------------------------------------------------
# AST walking helpers
# ---------------------------------------------------------------------------

@dataclass
class _NodeInfo:
    variable: str | None
    label: str | None
    line: int | None


@dataclass
class _RelInfo:
    src_var: str | None
    src_label: str | None
    dst_var: str | None
    dst_label: str | None
    rel_type: str | None
    direction: str  # "right" (-[]->) or "left" (<-[]-)
    line: int | None


@dataclass
class _PatternInfo:
    nodes: list[_NodeInfo]
    relationships: list[_RelInfo]


@dataclass
class _ProjectionItem:
    expression: str
    alias: str | None


@dataclass
class _Clause:
    kind: str  # "match", "with", "return", "unwind", "where", "order_by"
    line: int | None
    patterns: list[_PatternInfo] = field(default_factory=list)
    projection_items: list[_ProjectionItem] = field(default_factory=list)
    expressions: list[str] = field(default_factory=list)
    unwind_alias: str | None = None
    unwind_source: str | None = None


class _VariableScope:
    def __init__(self) -> None:
        self.variables: dict[str, str | None] = {}  # var -> label
        self._dropped: dict[str, bool] = {}

    def bind(self, var: str, label: str | None) -> None:
        self.variables[var] = label
        self._dropped.pop(var, None)

    def label_of(self, var: str | None) -> str | None:
        if var is None:
            return None
        return self.variables.get(var)

    def project_to(self, keep: set[str]) -> None:
        to_drop = [v for v in self.variables if v not in keep]
        for v in to_drop:
            self._dropped[v] = True
            del self.variables[v]

    def project_to_new(self, new_vars: dict[str, str | None]) -> None:
        """Replace the entire scope with a new set of variables.

        Used by WITH ... AS ... where aliases create new names.
        """
        for v in list(self.variables):
            if v not in new_vars:
                self._dropped[v] = True
        self.variables = dict(new_vars)

    def dropped_at(self, var: str) -> bool:
        return self._dropped.get(var, False)


def _extract_clauses(ast: CypherAst) -> list[_Clause]:
    """Walk the AST and extract a flat list of clauses."""
    clauses: list[_Clause] = []
    _walk_for_clauses(ast.tree, clauses)
    return clauses


def _walk_for_clauses(node: Any, clauses: list[_Clause]) -> None:
    name = type(node).__name__

    if name == "MatchStContext":
        clause = _Clause(kind="match", line=_line_of(node))
        pattern = _extract_pattern(node)
        if pattern:
            clause.patterns.append(pattern)
        clauses.append(clause)
        return

    if name == "WithStContext":
        clause = _Clause(kind="with", line=_line_of(node))
        clause.projection_items = _extract_projection_items(node)
        clauses.append(clause)
        # WHERE inside WITH (e.g., WITH x WHERE ...) is a child of
        # WithStContext. Extract it as a separate where clause so scope
        # checking applies after the WITH projection.
        children = getattr(node, "children", None)
        if children:
            for child in children:
                if type(child).__name__ == "WhereContext":
                    where_clause = _Clause(kind="where", line=_line_of(child))
                    where_clause.expressions = [child.getText()]
                    clauses.append(where_clause)
        return

    if name == "ReturnStContext":
        clause = _Clause(kind="return", line=_line_of(node))
        clause.projection_items = _extract_projection_items(node)
        clauses.append(clause)
        # ORDER BY is nested inside ReturnStContext -> ProjectionBodyContext.
        # Extract it as a separate clause for scope checking.
        def _find_order(n: Any) -> None:
            for child in getattr(n, "children", []):
                if type(child).__name__ == "OrderStContext":
                    order_clause = _Clause(kind="order_by", line=_line_of(child))
                    order_clause.expressions = [child.getText()]
                    clauses.append(order_clause)
                    return
                _find_order(child)
        _find_order(node)
        return

    if name == "UnwindStContext":
        clause = _Clause(kind="unwind", line=_line_of(node))
        source, alias = _extract_unwind_source_and_alias(node)
        clause.unwind_source = source
        clause.unwind_alias = alias
        clauses.append(clause)
        return

    if name == "WhereContext":
        # WhereContext appears inside PatternWhereContext (part of MATCH
        # pattern — already handled) and inside WithStContext (standalone
        # WHERE after WITH — needs scope checking). Only capture the latter.
        parent = getattr(node, "parentCtx", None)
        parent_name = type(parent).__name__ if parent else ""
        if parent_name != "PatternWhereContext":
            clause = _Clause(kind="where", line=_line_of(node))
            clause.expressions = [node.getText()]
            clauses.append(clause)
            return

    if name == "OrderStContext":
        clause = _Clause(kind="order_by", line=_line_of(node))
        clause.expressions = [node.getText()]
        clauses.append(clause)
        return

    children = getattr(node, "children", None)
    if children:
        for child in children:
            _walk_for_clauses(child, clauses)


def _extract_pattern(match_ctx: Any) -> _PatternInfo | None:
    """Extract nodes and relationships from a MatchStContext."""
    nodes: list[_NodeInfo] = []
    relationships: list[_RelInfo] = []

    def walk(node: Any) -> None:
        name = type(node).__name__

        if name == "NodePatternContext":
            var, label = _extract_node_var_label(node)
            nodes.append(_NodeInfo(variable=var, label=label, line=_line_of(node)))
            return

        if name == "PatternElemChainContext":
            _extract_relationship_chain(node, nodes, relationships)
            return

        children = getattr(node, "children", None)
        if children:
            for child in children:
                walk(child)

    walk(match_ctx)
    return _PatternInfo(nodes=nodes, relationships=relationships)


def _extract_node_var_label(node_ctx: Any) -> tuple[str | None, str | None]:
    """Extract variable name and label from a NodePatternContext."""
    variable = None
    label = None
    children = getattr(node_ctx, "children", None)
    if not children:
        return variable, label
    for child in children:
        name = type(child).__name__
        if name == "SymbolContext":
            variable = child.getText()
        elif name == "NodeLabelsContext":
            label = _extract_first_label(child)
    return variable, label


def _extract_first_label(labels_ctx: Any) -> str | None:
    """Extract the first label from NodeLabelsContext."""
    children = getattr(labels_ctx, "children", None)
    if not children:
        return None
    for child in children:
        name = type(child).__name__
        if name == "NameContext":
            return child.getText()
    return None


def _extract_relationship_chain(
    chain_ctx: Any,
    nodes: list[_NodeInfo],
    relationships: list[_RelInfo],
) -> None:
    """Extract relationship and target node from PatternElemChainContext."""
    rel_type = None
    direction = "right"
    rel_line = _line_of(chain_ctx)

    children = getattr(chain_ctx, "children", None)
    if not children:
        return

    for child in children:
        child_name = type(child).__name__
        if child_name == "RelationshipPatternContext":
            rel_type, direction = _extract_rel_type_and_direction(child)
            rel_line = _line_of(child)
        elif child_name == "NodePatternContext":
            var, label = _extract_node_var_label(child)
            target_node = _NodeInfo(variable=var, label=label, line=_line_of(child))
            nodes.append(target_node)

            # Build relationship info
            src_node = nodes[-2] if len(nodes) >= 2 else _NodeInfo(None, None, None)
            relationships.append(_RelInfo(
                src_var=src_node.variable,
                src_label=src_node.label,
                dst_var=target_node.variable,
                dst_label=target_node.label,
                rel_type=rel_type,
                direction=direction,
                line=rel_line,
            ))


def _extract_rel_type_and_direction(rel_ctx: Any) -> tuple[str | None, str]:
    """Extract relationship type and direction from RelationshipPatternContext."""
    rel_type = None
    has_left_arrow = False
    has_right_arrow = False

    children = getattr(rel_ctx, "children", None)
    if not children:
        return None, "right"

    for child in children:
        child_name = type(child).__name__
        if child_name == "RelationDetailContext":
            rel_type = _extract_rel_type_from_detail(child)
        elif child_name == "TerminalNodeImpl":
            text = child.getText()
            if text == "<":
                has_left_arrow = True
            elif text == ">":
                has_right_arrow = True

    if has_left_arrow and not has_right_arrow:
        return rel_type, "left"
    return rel_type, "right"


def _extract_rel_type_from_detail(detail_ctx: Any) -> str | None:
    """Extract relationship type name from RelationDetailContext."""
    children = getattr(detail_ctx, "children", None)
    if not children:
        return None
    for child in children:
        child_name = type(child).__name__
        if child_name == "RelationshipTypesContext":
            # Find NameContext inside
            for gc in getattr(child, "children", []):
                if type(gc).__name__ == "NameContext":
                    return gc.getText()
    return None


def _extract_projection_items(ctx: Any) -> list[_ProjectionItem]:
    """Extract projection items from WithStContext or ReturnStContext."""
    items: list[_ProjectionItem] = []

    def walk(node: Any) -> None:
        name = type(node).__name__
        if name == "ProjectionItemContext":
            expr, alias = _extract_projection_expr_alias(node)
            items.append(_ProjectionItem(expression=expr, alias=alias))
            return
        children = getattr(node, "children", None)
        if children:
            for child in children:
                walk(child)

    walk(ctx)
    return items


def _extract_projection_expr_alias(item_ctx: Any) -> tuple[str, str | None]:
    """Extract expression and optional alias from ProjectionItemContext."""
    children = getattr(item_ctx, "children", None)
    if not children:
        return item_ctx.getText(), None

    # If there's an AS keyword, everything before AS is expr, after is alias
    texts = []
    alias = None
    found_as = False
    for child in children:
        child_text = child.getText()
        if child_text.upper() == "AS":
            found_as = True
            continue
        if child_text.strip() == "":
            continue
        if found_as:
            alias = child_text
        else:
            texts.append(child_text)

    expr = "".join(texts) if texts else item_ctx.getText()
    return expr, alias


def _extract_unwind_source_and_alias(ctx: Any) -> tuple[str | None, str | None]:
    """Extract source expression and alias from UNWIND expr AS alias."""
    text = ctx.getText()
    # Strip leading UNWIND keyword
    upper = text.upper()
    if upper.startswith("UNWIND"):
        text = text[6:].strip()
        upper = text.upper()
    as_idx = upper.rfind("AS")
    if as_idx >= 0:
        source = text[:as_idx].strip() or None
        alias = text[as_idx + 2:].strip() or None
        return source, alias
    return text.strip() or None, None


def _extract_variable_refs(expression: str) -> list[str]:
    """Extract variable references from a Cypher expression.

    Uses a heuristic: finds identifiers that appear before '[' (property
    access) which is the primary pattern for variable references in
    Memgraph Cypher (e.g., ns['metadata']['name']).

    Also captures standalone identifiers used in WITH/RETURN projections
    (e.g., 'WITH ns, pvc' → ['ns', 'pvc']).

    Excludes quoted strings, Cypher keywords, and function names.
    """
    import re
    # First strip out quoted strings to avoid matching their contents
    stripped = re.sub(r"'[^']*'", "''", expression)
    stripped = re.sub(r'"[^"]*"', '""', stripped)

    # Primary pattern: identifier followed by [ (property access)
    prop_refs = re.findall(r"\b([a-zA-Z_]\w*)\s*\[", stripped)

    # Secondary: standalone identifiers (for WITH/RETURN projections)
    all_ids = re.findall(r"\b([a-zA-Z_]\w*)\b", stripped)

    # Merge and deduplicate, preserving order
    seen: set[str] = set()
    refs: list[str] = []
    for r in prop_refs + all_ids:
        if r not in seen:
            seen.add(r)
            refs.append(r)

    # Filter out Cypher keywords, functions, and common property names
    keywords = {
        "AS", "as", "DISTINCT", "distinct", "NULL", "null", "TRUE",
        "true", "FALSE", "false", "count", "collect", "sum", "avg",
        "min", "max", "size", "coalesce", "toFloat", "toInteger",
        "toString", "replace", "round", "abs",
        "CASE", "WHEN", "THEN", "ELSE", "END",
        "IS", "NOT", "AND", "OR", "IN",
        "STARTS", "ENDS", "WITH", "CONTAINS",
        "WHERE", "MATCH", "OPTIONAL", "RETURN", "ORDER", "BY",
        "LIMIT", "SKIP", "UNWIND", "CREATE", "DELETE", "SET",
        "MERGE", "REMOVE", "DETACH", "DESC", "ASC",
        "metadata", "name", "namespace", "status", "spec",
        "phase", "type", "reason", "note",
    }
    return [r for r in refs if r not in keywords]


def _line_of(ctx: Any) -> int | None:
    start = getattr(ctx, "start", None)
    if start is not None:
        return getattr(start, "line", None)
    return None
