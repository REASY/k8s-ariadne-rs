from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import EvalQuestion


def load_dataset(path: Path) -> list[EvalQuestion]:
    suffix = path.suffix.lower()
    raw = _read_payload(path, suffix)
    if isinstance(raw, list):
        return [EvalQuestion.model_validate(item) for item in raw]
    if isinstance(raw, dict):
        return _load_grouped_dataset(raw)
    raise ValueError("Dataset must be a list of questions or a grouped mapping")


def _load_grouped_dataset(raw: dict[str, Any]) -> list[EvalQuestion]:
    questions: list[EvalQuestion] = []
    for group, items in raw.items():
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            tags = item.get("tags")
            if not isinstance(tags, list):
                tags = []
            tags = list(tags)
            tags.append(f"difficulty:{group}")
            item = dict(item)
            item["tags"] = tags
            questions.append(EvalQuestion.model_validate(item))
    if not questions:
        raise ValueError("Dataset groups did not contain any questions")
    return questions


def _read_payload(path: Path, suffix: str) -> Any:
    if suffix in {".yaml", ".yml"}:
        import yaml

        return yaml.safe_load(path.read_text(encoding="utf-8"))
    if suffix == ".json":
        import json

        return json.loads(path.read_text(encoding="utf-8"))
    raise ValueError(f"Unsupported dataset format: {suffix}")
