from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Sequence, TypeAlias

JsonValue: TypeAlias = (
    None | bool | int | float | str | Mapping[str, "JsonValue"] | Sequence["JsonValue"]
)
JsonObject: TypeAlias = dict[str, JsonValue]


@dataclass(frozen=True)
class CypherQuery:
    text: str
    params: JsonObject | None = None


@dataclass(frozen=True)
class AgentAnswer:
    question: str
    cypher: str
    result: JsonValue
    response: str
