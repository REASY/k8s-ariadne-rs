from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ExpectedResult(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    ordered: bool = False


class EvalQuestion(BaseModel):
    id: str
    question: str
    tags: list[str] = Field(default_factory=list)
    expected: ExpectedResult | None = None
    deterministic: bool = False
    reference_cypher: str | None = None
