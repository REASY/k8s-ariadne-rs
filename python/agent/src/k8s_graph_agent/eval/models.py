from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ComparisonPolicy(BaseModel):
    shape_policy: Literal["association_equivalent"] | None = None
    empty_parent_policy: Literal["allow_omitted", "allow_extra_empty"] | None = None
    zero_count_policy: Literal["allow_extra_zero_rows"] | None = None
    entity_name_policy: Literal["allow_full_entity_name_projection"] | None = None


class ExpectedResult(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    ordered: bool = False
    comparison: ComparisonPolicy | None = None


class EvalQuestion(BaseModel):
    id: str
    question: str
    tags: list[str] = Field(default_factory=list)
    expected: ExpectedResult | None = None
    deterministic: bool = False
    reference_cypher: str | None = None
    traversal_plan: str | None = None
    family: str | None = None
    group_id: str | None = None
    source_question_id: str | None = None
    generation_type: str | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
