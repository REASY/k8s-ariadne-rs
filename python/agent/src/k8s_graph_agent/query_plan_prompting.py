from __future__ import annotations

from dataclasses import dataclass
import re

from .graph_schema import GraphSchema
from .query_plan import EntityType, RelationshipType
from .query_plan_validator import (
    query_plan_entity_properties,
    query_plan_unwind_properties,
)


_CONNECTIVITY_LINE_PATTERN = re.compile(
    r"\(:(?P<src>[A-Za-z_][\w]*)\)\s*-\s*\[:(?P<rel>[A-Za-z_][\w]*)\]\s*->\s*\(:(?P<dst>[A-Za-z_][\w]*)\)"
)


@dataclass(frozen=True)
class DistilledIrPromptContext:
    entity_properties: str
    relationship_table: str
    connectivity_excerpt: str

    def render(self) -> str:
        return (
            "Distilled QueryPlanV1 reference\n\n"
            "Logical entity properties\n"
            f"{self.entity_properties}\n\n"
            "Allowed relationships\n"
            f"{self.relationship_table}\n\n"
            "Relevant connectivity excerpt\n"
            f"{self.connectivity_excerpt}"
        )


def build_distilled_ir_prompt_context(
    *,
    node_connectivity: str,
    schema: GraphSchema,
) -> DistilledIrPromptContext:
    return DistilledIrPromptContext(
        entity_properties=_render_entity_properties(),
        relationship_table=_render_relationship_table(schema),
        connectivity_excerpt=_render_connectivity_excerpt(node_connectivity, schema),
    )


def _render_entity_properties() -> str:
    lines: list[str] = []
    for entity_name, properties in query_plan_entity_properties().items():
        lines.append(f"- {entity_name}: {', '.join(properties)}")
    for unwind_name, properties in query_plan_unwind_properties().items():
        lines.append(f"- {unwind_name}: {', '.join(properties)}")
    return "\n".join(lines)


def _render_relationship_table(schema: GraphSchema) -> str:
    supported_rels = {rel.value for rel in RelationshipType}
    supported_entities = {entity.value for entity in EntityType}
    rows: list[tuple[str, str, str]] = []
    for rel_name, pairs in schema.relationships.items():
        if rel_name not in supported_rels:
            continue
        for src, dst in pairs:
            if src in supported_entities and dst in supported_entities:
                rows.append((src, rel_name, dst))
    if not rows:
        return "- none"
    return "\n".join(
        f"- {src} -[:{rel}]-> {dst}"
        for src, rel, dst in sorted(rows)
    )


def _render_connectivity_excerpt(node_connectivity: str, schema: GraphSchema) -> str:
    supported_rels = {rel.value for rel in RelationshipType}
    supported_entities = {entity.value for entity in EntityType}
    kept: list[str] = []
    for raw_line in node_connectivity.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _CONNECTIVITY_LINE_PATTERN.search(line)
        if not match:
            continue
        src = match.group("src")
        rel = match.group("rel")
        dst = match.group("dst")
        if (
            rel in supported_rels
            and src in supported_entities
            and dst in supported_entities
            and schema.allows(rel, src, dst)
        ):
            kept.append(f"- {src} -[:{rel}]-> {dst}")
    if not kept:
        return _render_relationship_table(schema)
    return "\n".join(dict.fromkeys(kept))
