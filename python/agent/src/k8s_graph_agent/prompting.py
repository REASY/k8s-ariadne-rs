from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, cast

from .models import JsonObject, JsonValue


_RULES_HEADER = "## Rules for Cypher query generation"
_SCHEMA_HEADER = "## Definitive Graph Schema Reference"
_CONNECTIVITY_HEADER = "### Node Connectivity"
_USER_QUESTION_MARKER = "\n\nUser question:"
_FINAL_CHECK_MARKER = "Before you output the final query"


@dataclass(frozen=True)
class PromptSections:
    instruction: str
    rules: str
    schema_reference: str
    node_connectivity: str
    footer: str

    @property
    def tunable_instruction(self) -> str:
        parts = [self.instruction.strip(), self.rules.strip(), self.footer.strip()]
        return "\n\n".join(part for part in parts if part)


def extract_prompt_text(prompt_result: JsonObject) -> str | None:
    messages = prompt_result.get("messages")
    if not isinstance(messages, list):
        return None
    texts: list[str] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        content = message.get("content")
        text = _extract_text_from_content(content)
        if text:
            texts.append(text)
    if not texts:
        return None
    return "\n".join(texts)


def _extract_text_from_content(content: object) -> str | None:
    if isinstance(content, dict):
        content_dict = cast(dict[str, JsonValue], content)
        content_type = content_dict.get("type")
        if content_type == "text":
            text = content_dict.get("text")
            if isinstance(text, str):
                return text
        text = content_dict.get("text")
        if isinstance(text, str):
            return text
        parts = content_dict.get("parts")
        if isinstance(parts, list):
            return _join_parts(parts)
    return None


def _join_parts(parts: Iterable[object]) -> str | None:
    collected: list[str] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        part_dict = cast(dict[str, JsonValue], part)
        text = part_dict.get("text")
        if isinstance(text, str):
            collected.append(text)
    if not collected:
        return None
    return "".join(collected)


def split_prompt_sections(prompt_text: str) -> PromptSections:
    prompt_body = _strip_user_question_suffix(prompt_text)
    rules_start = prompt_body.find(_RULES_HEADER)
    schema_start = prompt_body.find(_SCHEMA_HEADER)
    connectivity_start = prompt_body.find(_CONNECTIVITY_HEADER)
    if min(rules_start, schema_start, connectivity_start) < 0:
        raise ValueError("Prompt is missing one or more expected section headers")
    footer_start = prompt_body.find(_FINAL_CHECK_MARKER, connectivity_start)

    instruction = prompt_body[:rules_start].strip()
    rules = prompt_body[rules_start:schema_start].strip()
    schema_reference = prompt_body[schema_start:connectivity_start].strip()
    if footer_start >= 0:
        node_connectivity = prompt_body[connectivity_start:footer_start].strip()
        footer = prompt_body[footer_start:].strip()
    else:
        node_connectivity = prompt_body[connectivity_start:].strip()
        footer = ""
    return PromptSections(
        instruction=instruction,
        rules=rules,
        schema_reference=schema_reference,
        node_connectivity=node_connectivity,
        footer=footer,
    )


def _strip_user_question_suffix(prompt_text: str) -> str:
    marker_index = prompt_text.find(_USER_QUESTION_MARKER)
    if marker_index >= 0:
        return prompt_text[:marker_index].rstrip()
    return prompt_text.rstrip()


def render_prompt_bundle(bundle_text: str, question: str) -> str:
    body = bundle_text.rstrip()
    escaped_question = question.replace("'", "\\'")
    return f"{body}\n\nUser question: '{escaped_question}'"


def prompt_sections_from_graph_schema_payload(
    payload: JsonObject,
) -> PromptSections | None:
    node_labels = payload.get("node_labels")
    relationship_types = payload.get("relationship_types") or payload.get(
        "relationships"
    )
    if not isinstance(node_labels, list) or not isinstance(relationship_types, list):
        return None

    schema_lines = ["## Definitive Graph Schema Reference", "Node labels:"]
    for item in node_labels:
        if not isinstance(item, dict):
            continue
        label = item.get("label")
        properties = item.get("properties")
        if not isinstance(label, str) or not isinstance(properties, list):
            continue
        formatted_props: list[str] = []
        for prop in properties:
            if not isinstance(prop, dict):
                continue
            name = prop.get("name")
            prop_type = prop.get("type")
            if isinstance(name, str) and isinstance(prop_type, str):
                formatted_props.append(f"{name}: {prop_type}")
        if formatted_props:
            schema_lines.append(f"- {label}: {', '.join(formatted_props)}")
        else:
            schema_lines.append(f"- {label}")

    connectivity_lines = ["### Node Connectivity"]
    for item in relationship_types:
        if not isinstance(item, dict):
            continue
        src = item.get("from") or item.get("src") or item.get("source")
        rel = item.get("edge") or item.get("type") or item.get("relationship")
        dst = item.get("to") or item.get("dst") or item.get("target")
        if isinstance(src, str) and isinstance(rel, str) and isinstance(dst, str):
            connectivity_lines.append(f"- (:{src})-[:{rel}]->(:{dst})")

    return PromptSections(
        instruction="You write read-only Cypher queries for the Kubernetes graph.",
        rules=(
            "## Rules for Cypher query generation\n"
            "- Use only labels, properties, and relationships present in the schema.\n"
            "- Prefer parameterized predicates (`$var`) for user-provided values.\n"
            "- Prefer `LIMIT` in exploratory queries.\n"
            "- Return only the fields needed to answer the question."
        ),
        schema_reference="\n".join(schema_lines),
        node_connectivity="\n".join(connectivity_lines),
        footer="Before you output the final query, ensure it is read-only and syntactically valid.",
    )
