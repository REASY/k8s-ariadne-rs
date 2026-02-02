from __future__ import annotations

from typing import Any, Iterable

from antlr4 import ParserRuleContext


def _iter_rule_contexts(
    tree: Any, parser: Any
) -> Iterable[tuple[str, ParserRuleContext, tuple[str, ...]]]:
    stack: list[tuple[Any, tuple[str, ...]]] = [(tree, ())]
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
