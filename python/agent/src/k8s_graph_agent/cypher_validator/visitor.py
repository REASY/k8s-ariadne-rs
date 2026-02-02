from __future__ import annotations

from antlr4 import ParserRuleContext
from antlr4_cypher import CypherParser
from antlr4_cypher.CypherParserVisitor import CypherParserVisitor

from .model import _NodeUse, _RelationshipUse


def _strip_wrapping(text: str, left: str, right: str) -> str:
    if text.startswith(left) and text.endswith(right):
        return text[1:-1]
    return text


def _clean_name(text: str) -> str:
    if len(text) >= 2 and text[0] == "`" and text[-1] == "`":
        return text[1:-1]
    return text


def _node_from_ctx(ctx: CypherParser.NodePatternContext) -> _NodeUse:
    text = _strip_wrapping(ctx.getText(), "(", ")")
    symbol_ctx = ctx.symbol()
    var = _clean_name(symbol_ctx.getText()) if symbol_ctx else None
    labels: list[str] = []
    labels_ctx = ctx.nodeLabels()
    if labels_ctx:
        for name_ctx in labels_ctx.name():
            labels.append(_clean_name(name_ctx.getText()))
    return _NodeUse(text=text, var=var, labels=tuple(labels))


def _relationship_types(
    ctx: CypherParser.RelationshipPatternContext,
) -> tuple[str, ...]:
    detail = ctx.relationDetail()
    if not detail:
        return ()
    types_ctx = detail.relationshipTypes()
    if not types_ctx:
        return ()
    return tuple(_clean_name(name_ctx.getText()) for name_ctx in types_ctx.name())


def _relationship_text(ctx: CypherParser.RelationshipPatternContext) -> str:
    detail = ctx.relationDetail()
    if not detail:
        return ""
    return _strip_wrapping(detail.getText(), "[", "]")


def _relationship_dirs(ctx: CypherParser.RelationshipPatternContext) -> tuple[str, str]:
    left_dir = "<-" if ctx.LT() else "-"
    right_dir = "->" if ctx.GT() else "-"
    return left_dir, right_dir


def _format_snippet(
    left_node_text: str,
    right_node_text: str,
    rel_text: str,
    left_dir: str,
    right_dir: str,
) -> str:
    return f"({left_node_text}){left_dir}[{rel_text}]{right_dir}({right_node_text})"


def _rule_path(ctx: ParserRuleContext, parser: CypherParser) -> str:
    parts: list[str] = []
    node: ParserRuleContext | None = ctx
    while node is not None:
        parts.append(parser.ruleNames[node.getRuleIndex()])
        parent = node.parentCtx
        if isinstance(parent, ParserRuleContext):
            node = parent
        else:
            node = None
    parts.reverse()
    return "/".join(parts)


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


class SchemaValidationVisitor(CypherParserVisitor):
    def __init__(self, parser: CypherParser) -> None:
        self._parser = parser
        self.variable_labels: dict[str, set[str]] = {}
        self.relationships: list[_RelationshipUse] = []

    def visitNodePattern(self, ctx: CypherParser.NodePatternContext):
        node = _node_from_ctx(ctx)
        if node.var and node.labels:
            self.variable_labels.setdefault(node.var, set()).update(node.labels)
        return self.visitChildren(ctx)

    def visitPatternElem(self, ctx: CypherParser.PatternElemContext):
        if ctx.nodePattern():
            self._collect_chain(ctx.nodePattern(), ctx.patternElemChain())
        return self.visitChildren(ctx)

    def visitRelationshipsChainPattern(
        self, ctx: CypherParser.RelationshipsChainPatternContext
    ):
        self._collect_chain(ctx.nodePattern(), ctx.patternElemChain())
        return self.visitChildren(ctx)

    def _collect_chain(
        self,
        start_node_ctx: CypherParser.NodePatternContext,
        chain_ctxs: list[CypherParser.PatternElemChainContext],
    ) -> None:
        current = _node_from_ctx(start_node_ctx)
        for chain_ctx in chain_ctxs:
            rel_ctx = chain_ctx.relationshipPattern()
            next_node_ctx = chain_ctx.nodePattern()
            next_node = _node_from_ctx(next_node_ctx)
            rel_types = _relationship_types(rel_ctx)
            rel_text = _relationship_text(rel_ctx)
            left_dir, right_dir = _relationship_dirs(rel_ctx)
            snippet = _format_snippet(
                current.text, next_node.text, rel_text, left_dir, right_dir
            )
            rule_path = _rule_path(rel_ctx, self._parser)
            self.relationships.append(
                _RelationshipUse(
                    left_node=current,
                    right_node=next_node,
                    rel_text=rel_text,
                    rel_types=rel_types,
                    left_dir=left_dir,
                    right_dir=right_dir,
                    snippet=snippet,
                    rule_path=rule_path,
                )
            )
            current = next_node
