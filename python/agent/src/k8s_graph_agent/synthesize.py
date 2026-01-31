from __future__ import annotations

import json
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
