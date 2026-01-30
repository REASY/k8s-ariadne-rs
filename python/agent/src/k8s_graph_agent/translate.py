from __future__ import annotations

from typing import Protocol

from .models import CypherQuery


class CypherTranslator(Protocol):
    def translate(self, question: str) -> CypherQuery:
        ...


class PrefixCypherTranslator:
    def translate(self, question: str) -> CypherQuery:
        normalized = question.strip()
        if not normalized:
            raise ValueError("question is empty")
        lower = normalized.lower()
        if lower.startswith("cypher:"):
            return CypherQuery(text=normalized.split(":", 1)[1].strip())
        if lower.startswith("match") or lower.startswith("with") or lower.startswith("call"):
            return CypherQuery(text=normalized)
        raise ValueError(
            "question does not look like Cypher; prefix with 'cypher:' to run directly"
        )
