from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import random
from typing import Any, Iterable, Mapping

import yaml

from ..agent import GraphMcpClient
from ..models import JsonValue
from .models import EvalQuestion, ExpectedResult


DatasetRaw = dict[str, Any] | list[dict[str, Any]]


@dataclass(frozen=True)
class QuestionLocation:
    group: str | None
    index: int
    item: dict[str, Any]


def load_dataset_raw(path: Path) -> DatasetRaw:
    suffix = path.suffix.lower()
    if suffix not in {".yaml", ".yml", ".json"}:
        raise ValueError(f"Unsupported dataset format: {suffix}")
    text = path.read_text(encoding="utf-8")
    if suffix in {".yaml", ".yml"}:
        raw = yaml.safe_load(text)
    else:
        raw = json.loads(text)
    if not isinstance(raw, (dict, list)):
        raise ValueError("Dataset must be a list of questions or a grouped mapping")
    return raw


def write_dataset_raw(path: Path, raw: DatasetRaw) -> None:
    suffix = path.suffix.lower()
    if suffix in {".yaml", ".yml"}:
        path.write_text(
            yaml.safe_dump(raw, sort_keys=False, allow_unicode=False),
            encoding="utf-8",
        )
        return
    if suffix == ".json":
        path.write_text(json.dumps(raw, indent=2), encoding="utf-8")
        return
    raise ValueError(f"Unsupported dataset format: {suffix}")


def sample_grouped_dataset(
    raw: DatasetRaw,
    *,
    total: int,
    seed: int,
    mark_deterministic: bool = True,
) -> dict[str, list[dict[str, Any]]]:
    if total <= 0:
        raise ValueError("total must be greater than zero")
    if not isinstance(raw, dict):
        raise ValueError("Stratified sampling requires a grouped dataset mapping")

    grouped_items: dict[str, list[dict[str, Any]]] = {}
    for group, items in raw.items():
        if not isinstance(items, list):
            continue
        group_rows = [dict(item) for item in items if isinstance(item, dict)]
        if group_rows:
            grouped_items[str(group)] = group_rows
    if not grouped_items:
        raise ValueError("Dataset groups did not contain any questions")

    total_available = sum(len(items) for items in grouped_items.values())
    if total > total_available:
        raise ValueError(
            f"Requested {total} questions but dataset only has {total_available}"
        )

    group_names = list(grouped_items.keys())
    allocation = _allocate_group_counts(
        {group: len(grouped_items[group]) for group in group_names},
        total=total,
    )
    rng = random.Random(seed)
    sampled: dict[str, list[dict[str, Any]]] = {}
    for group in group_names:
        count = allocation[group]
        if count <= 0:
            continue
        chosen = rng.sample(grouped_items[group], count)
        chosen.sort(key=lambda item: str(item.get("id", "")))
        if mark_deterministic:
            for item in chosen:
                item["deterministic"] = True
        sampled[group] = chosen
    return sampled


def iter_question_locations(raw: DatasetRaw) -> Iterable[QuestionLocation]:
    if isinstance(raw, list):
        for index, item in enumerate(raw):
            if isinstance(item, dict):
                yield QuestionLocation(group=None, index=index, item=item)
        return

    for group, items in raw.items():
        if not isinstance(items, list):
            continue
        for index, item in enumerate(items):
            if isinstance(item, dict):
                yield QuestionLocation(group=group, index=index, item=item)


def collect_questions(raw: DatasetRaw) -> list[EvalQuestion]:
    questions: list[EvalQuestion] = []
    for location in iter_question_locations(raw):
        item = dict(location.item)
        tags = item.get("tags")
        if not isinstance(tags, list):
            tags = []
        tags = [str(tag) for tag in tags]
        if location.group:
            tags = tags + [f"difficulty:{location.group}"]
        item["tags"] = tags
        questions.append(EvalQuestion.model_validate(item))
    return questions


def select_questions(
    raw: DatasetRaw,
    *,
    ids: set[str] | None = None,
    tags: set[str] | None = None,
    deterministic_only: bool = False,
    require_reference: bool = False,
) -> list[EvalQuestion]:
    selected: list[EvalQuestion] = []
    for question in collect_questions(raw):
        if ids is not None and question.id not in ids:
            continue
        if tags is not None and not tags.intersection(question.tags):
            continue
        if deterministic_only and not question.deterministic:
            continue
        if require_reference and not question.reference_cypher:
            continue
        selected.append(question)
    return selected


def update_question_fields(
    raw: DatasetRaw, question_id: str, fields: Mapping[str, Any]
) -> bool:
    for location in iter_question_locations(raw):
        if location.item.get("id") != question_id:
            continue
        location.item.update(fields)
        return True
    return False


def build_expected_result(
    result: JsonValue,
    *,
    columns: list[str] | None = None,
    ordered: bool = False,
) -> ExpectedResult:
    normalized_rows = _rows_from_result(result)
    if not columns:
        columns = _infer_columns(normalized_rows)
    rows = [[row.get(column) for column in columns] for row in normalized_rows]
    return ExpectedResult(columns=columns, rows=rows, ordered=ordered)


def result_fingerprint(result: JsonValue) -> str | None:
    try:
        payload = _canonicalize_result(result)
    except ValueError:
        return None
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return digest[:16]


def consensus_fingerprint(values: Iterable[str | None]) -> str | None:
    counts = Counter(value for value in values if value)
    if not counts:
        return None
    fingerprint, _count = counts.most_common(1)[0]
    return fingerprint


def execute_reference_query(
    graph: GraphMcpClient,
    question: EvalQuestion,
    *,
    ordered: bool | None = None,
) -> ExpectedResult:
    reference_cypher = question.reference_cypher
    if not reference_cypher:
        raise ValueError(f"Question {question.id} has no reference_cypher")
    existing_expected = question.expected
    effective_ordered = (
        ordered if ordered is not None else existing_expected.ordered
        if existing_expected is not None
        else False
    )
    existing_columns = existing_expected.columns if existing_expected is not None else None
    result = graph.execute_cypher(reference_cypher)
    return build_expected_result(
        result,
        columns=existing_columns,
        ordered=effective_ordered,
    )


def choose_consensus_reference(
    candidate_runs: Iterable[Mapping[str, Any]],
    *,
    min_models: int = 2,
) -> str | None:
    eligible_runs = []
    for run in candidate_runs:
        fingerprint = run.get("result_fingerprint")
        cypher = run.get("cypher")
        if not isinstance(fingerprint, str) or not fingerprint:
            continue
        if not isinstance(cypher, str) or not cypher.strip():
            continue
        if run.get("execution_error"):
            continue
        if run.get("valid") is False:
            continue
        eligible_runs.append(run)
    if not eligible_runs:
        return None

    fingerprint_counts = Counter(run["result_fingerprint"] for run in eligible_runs)
    consensus_fingerprints = {
        fingerprint
        for fingerprint, count in fingerprint_counts.items()
        if count >= min_models
    }
    if len(consensus_fingerprints) != 1:
        return None

    consensus_fingerprint_value = next(iter(consensus_fingerprints))
    cypher_counts = Counter(
        str(run["cypher"]).strip()
        for run in eligible_runs
        if run["result_fingerprint"] == consensus_fingerprint_value
    )
    if len(cypher_counts) != 1:
        return None
    cypher, count = cypher_counts.most_common(1)[0]
    if count < min_models:
        return None
    return cypher


def _rows_from_result(result: JsonValue) -> list[dict[str, Any]]:
    if isinstance(result, list):
        rows: list[dict[str, Any]] = []
        for row in result:
            if not isinstance(row, Mapping):
                raise ValueError("Expected list results to contain row objects")
            rows.append(dict(row))
        return rows
    if isinstance(result, Mapping):
        return [dict(result)]
    raise ValueError("Expected query result to be an object or list of objects")


def _infer_columns(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return []
    return [str(key) for key in rows[0].keys()]


def _canonicalize_result(result: JsonValue) -> str:
    rows = _rows_from_result(result)
    canonical_rows = [{key: row.get(key) for key in sorted(row.keys())} for row in rows]
    canonical_rows.sort(key=lambda item: json.dumps(item, sort_keys=True, default=str))
    return json.dumps(canonical_rows, sort_keys=True, default=str)


def _allocate_group_counts(group_sizes: Mapping[str, int], *, total: int) -> dict[str, int]:
    if total < 0:
        raise ValueError("total must be non-negative")
    allocation = {group: 0 for group in group_sizes}
    remaining = total
    groups = list(group_sizes.keys())
    while remaining > 0:
        eligible = [group for group in groups if allocation[group] < group_sizes[group]]
        if not eligible:
            break
        base = remaining // len(eligible)
        if base <= 0:
            base = 1
        progressed = False
        for group in eligible:
            if remaining <= 0:
                break
            room = group_sizes[group] - allocation[group]
            if room <= 0:
                continue
            increment = min(base, room, remaining)
            if increment <= 0:
                continue
            allocation[group] += increment
            remaining -= increment
            progressed = True
        if not progressed:
            raise ValueError("Failed to allocate group counts")
    if remaining != 0:
        raise ValueError("Failed to allocate all requested samples")
    return allocation
