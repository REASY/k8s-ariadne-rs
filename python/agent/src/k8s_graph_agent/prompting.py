from __future__ import annotations

from typing import Iterable

from .models import JsonObject


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
        content_type = content.get("type")
        if content_type == "text":
            text = content.get("text")
            if isinstance(text, str):
                return text
        text = content.get("text")
        if isinstance(text, str):
            return text
        parts = content.get("parts")
        if isinstance(parts, list):
            return _join_parts(parts)
    return None


def _join_parts(parts: Iterable[object]) -> str | None:
    collected: list[str] = []
    for part in parts:
        if not isinstance(part, dict):
            continue
        text = part.get("text")
        if isinstance(text, str):
            collected.append(text)
    if not collected:
        return None
    return "".join(collected)
