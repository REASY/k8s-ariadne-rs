from __future__ import annotations

import json
import re
from typing import Protocol

from .models import JsonValue


class ResponseSynthesizer(Protocol):
    def synthesize(self, question: str, cypher: str, result: JsonValue) -> str: ...


class SimpleResponseSynthesizer:
    def synthesize(self, question: str, cypher: str, result: JsonValue) -> str:
        if isinstance(result, list):
            count = len(result)
            summary = f"Cypher executed. Returned {count} row(s)."
            if count == 0:
                return summary
            sample = json.dumps(result[0], ensure_ascii=True)
            return f"{summary} Sample row: {sample}"
        if isinstance(result, dict):
            payload = json.dumps(result, ensure_ascii=True)
            return f"Cypher executed. Result: {payload}"
        return f"Cypher executed. Result: {result}"


class SreResponseSynthesizer:
    def synthesize(self, question: str, cypher: str, result: JsonValue) -> str:
        row_count, sample_keys = _summarize_result(result)
        lines = [
            "Facts:",
            f"- Question: {question}",
            f"- Cypher: {cypher}",
            f"- Rows returned: {row_count}",
        ]
        if sample_keys:
            lines.append(f"- Sample keys: {', '.join(sample_keys)}")
        lines.extend(
            [
                "",
                "Interpretation:",
                "- Result is a snapshot of the current graph state.",
            ]
        )
        if row_count == 0:
            lines.append("- No matching entities were found for this query.")
        lines.extend(
            [
                "",
                "Next steps:",
                "- Refine filters (namespace, labels, names) if results are too broad or empty.",
                "- Inspect related resources (pods, events, logs) based on returned entities.",
                "- If you need a different view, ask for a narrower or time-scoped query.",
            ]
        )
        return "\n".join(lines)


class WebUiResponseSynthesizer:
    def __init__(
        self,
        max_rows: int = 25,
        include_cypher: bool = False,
        cypher_fence: bool = True,
        cypher_format: str = "pretty",
        max_cell_chars: int = 120,
        compact_values: bool = True,
    ) -> None:
        self._max_rows = max_rows
        self._include_cypher = include_cypher
        self._cypher_fence = cypher_fence
        self._cypher_format = cypher_format
        self._max_cell_chars = max_cell_chars
        self._compact_values = compact_values

    def synthesize(self, question: str, cypher: str, result: JsonValue) -> str:
        lines: list[str] = []
        row_count, sample_keys = _summarize_result(result)
        if self._include_cypher:
            cypher_text = cypher
            if self._cypher_format == "pretty":
                cypher_text = _pretty_cypher(cypher_text)
            if self._cypher_fence:
                lines.append("Cypher:")
                lines.append("```cypher")
                lines.append(cypher_text)
                lines.append("```")
            else:
                lines.append(f"Cypher: `{cypher_text}`")
        lines.append(f"Rows: {row_count}")
        if isinstance(result, list):
            rows = [row for row in result if isinstance(row, dict)]
            if rows:
                table = _format_markdown_table(
                    rows,
                    self._max_rows,
                    self._max_cell_chars,
                    self._compact_values,
                )
                if table:
                    lines.append("")
                    lines.append(table)
                    if len(rows) > self._max_rows:
                        lines.append(
                            f"...showing first {self._max_rows} of {len(rows)} rows"
                        )
                return "\n".join(lines)
            if result:
                lines.append("")
                lines.append(json.dumps(result[: self._max_rows], ensure_ascii=True))
                if len(result) > self._max_rows:
                    lines.append(
                        f"...showing first {self._max_rows} of {len(result)} rows"
                    )
                return "\n".join(lines)
        if isinstance(result, dict):
            lines.append("")
            lines.append(json.dumps(result, ensure_ascii=True))
            return "\n".join(lines)
        if result is None:
            return "\n".join(lines)
        lines.append("")
        lines.append(json.dumps(result, ensure_ascii=True))
        if sample_keys:
            lines.append("")
            lines.append(f"Sample keys: {', '.join(sample_keys)}")
        return "\n".join(lines)


def _summarize_result(result: JsonValue) -> tuple[int, list[str]]:
    if isinstance(result, list):
        if not result:
            return 0, []
        sample = result[0]
        keys = list(sample.keys()) if isinstance(sample, dict) else []
        return len(result), keys
    if isinstance(result, dict):
        return 1, list(result.keys())
    if result is None:
        return 0, []
    return 1, []


def _format_markdown_table(
    rows: list[dict[str, object]],
    max_rows: int,
    max_cell_chars: int,
    compact_values: bool,
) -> str | None:
    if not rows:
        return None
    limited = rows[:max_rows]
    columns = _collect_columns(limited)
    if not columns:
        return None
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, sep]
    for row in limited:
        line = "| " + " | ".join(
            _format_markdown_cell_with_options(
                row.get(col),
                max_chars=max_cell_chars,
                compact=compact_values,
            )
            for col in columns
        ) + " |"
        lines.append(line)
    return "\n".join(lines)


def _collect_columns(rows: list[dict[str, object]]) -> list[str]:
    columns: list[str] = []
    seen = set()
    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                columns.append(key)
    return columns


def _format_cell(value: object) -> str:
    return _format_cell_with_options(value, max_chars=80, compact=True)


def _format_markdown_cell(value: object) -> str:
    return _format_markdown_cell_with_options(value, max_chars=120, compact=True)


def _format_markdown_cell_with_options(
    value: object, max_chars: int, compact: bool
) -> str:
    text = _format_cell_with_options(value, max_chars=max_chars, compact=compact)
    text = text.replace("|", "\\|").replace("\n", " ")
    return text


def _format_cell_with_options(
    value: object, max_chars: int, compact: bool
) -> str:
    if compact:
        compact_text = _compact_value(value)
        if compact_text is not None:
            return _truncate(compact_text, max_chars)
    text = json.dumps(value, ensure_ascii=True)
    return _truncate(text, max_chars)


def _truncate(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return text
    if len(text) > max_chars:
        return text[: max_chars - 3] + "..."
    return text


def _compact_value(value: object) -> str | None:
    if isinstance(value, dict):
        node = _summarize_graph_node(value)
        if node:
            return node
        summary = _summarize_k8s_object(value)
        if summary:
            return summary
        if len(value) > 8:
            keys = list(value.keys())[:5]
            return "{" + ", ".join(f"{k}=…" for k in keys) + ", …}"
    if isinstance(value, list):
        if not value:
            return "[]"
        if all(isinstance(item, str) for item in value[:5]):
            preview = ", ".join(value[:5])
            suffix = ", …" if len(value) > 5 else ""
            return f"[{preview}{suffix}]"
        if len(value) > 10:
            return f"[{len(value)} items]"
    return None


def _summarize_graph_node(value: dict[str, object]) -> str | None:
    labels = value.get("labels")
    props = value.get("properties")
    if not isinstance(labels, list) or not isinstance(props, dict):
        return None
    label = labels[0] if labels else "Node"
    name, namespace = _extract_k8s_name_namespace(props)
    phase = _extract_k8s_phase(props)
    if name and namespace:
        base = f"{label} {namespace}/{name}"
    elif name:
        base = f"{label} {name}"
    else:
        node_id = value.get("id")
        base = f"{label}#{node_id}" if node_id is not None else label
    if phase:
        base = f"{base} ({phase})"
    return base


def _summarize_k8s_object(value: dict[str, object]) -> str | None:
    name, namespace = _extract_k8s_name_namespace(value)
    if name and namespace:
        return f"{namespace}/{name}"
    if name:
        return str(name)
    return None


def _extract_k8s_name_namespace(
    props: dict[str, object],
) -> tuple[str | None, str | None]:
    metadata = props.get("metadata")
    if isinstance(metadata, dict):
        name = metadata.get("name")
        namespace = metadata.get("namespace")
        return _to_str(name), _to_str(namespace)
    name = props.get("name")
    namespace = props.get("namespace")
    return _to_str(name), _to_str(namespace)


def _extract_k8s_phase(props: dict[str, object]) -> str | None:
    status = props.get("status")
    if isinstance(status, dict):
        return _to_str(status.get("phase"))
    return None


def _to_str(value: object) -> str | None:
    if isinstance(value, str):
        return value
    return None


def _pretty_cypher(cypher: str) -> str:
    text = cypher.strip().rstrip(";")
    if not text:
        return text
    keywords = [
        "OPTIONAL MATCH",
        "MATCH",
        "UNWIND",
        "WHERE",
        "WITH",
        "RETURN",
        "ORDER BY",
        "SKIP",
        "LIMIT",
    ]
    def _normalize_ws(value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    normalized = _normalize_ws(text)
    for kw in sorted(keywords, key=len, reverse=True):
        normalized = re.sub(
            rf"(?i)\b{re.escape(kw)}\b", f"\n{kw}", normalized
        )
    return normalized.strip()
