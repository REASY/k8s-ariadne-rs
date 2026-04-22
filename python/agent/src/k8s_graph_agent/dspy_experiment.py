from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import random
from typing import Any, Iterable

import dspy

from .agent import GraphMcpClient
from .eval.bootstrap import collect_questions, load_dataset_raw
from .eval.matching import MatchType, evaluate_expected_match, MatchEvaluation, match_expected
from .eval.models import EvalQuestion, ExpectedResult
from .mcp_client import StreamableHttpMcpClient, extract_json_content
from .prompting import PromptSections, prompt_sections_from_graph_schema_payload


@dataclass(frozen=True)
class DspyDatasetSplit:
    train: list[EvalQuestion]
    dev: list[EvalQuestion]


@dataclass(frozen=True)
class EvalCounts:
    total: int
    valid: int
    exec_error: int
    exact_match: int
    invalid: int
    exact_column_match: int = 0
    projected_match: int = 0
    match_type_counts: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "valid": self.valid,
            "exec_error": self.exec_error,
            "matched": self.exact_match,
            "exact_match": self.exact_match,
            "invalid": self.invalid,
            "exact_column_match": self.exact_column_match,
            "projected_match": self.projected_match,
            "match_type_counts": dict(self.match_type_counts),
            "valid_rate": _ratio(self.valid, self.total),
            "exec_error_rate": _ratio(self.exec_error, self.total),
            "matched_rate": _ratio(self.exact_match, self.total),
            "exact_match_rate": _ratio(self.exact_match, self.total),
            "exact_column_match_rate": _ratio(self.exact_column_match, self.total),
            "projected_match_rate": _ratio(self.projected_match, self.total),
        }


@dataclass(frozen=True)
class ModelEvalResult:
    counts: EvalCounts
    avg_latency_ms: float
    rows_by_question: dict[str, dict[str, Any]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "counts": self.counts.as_dict(),
            "avg_latency_ms": self.avg_latency_ms,
            "rows_by_question": self.rows_by_question,
        }


def build_stratified_split(
    dataset_path: Path, *, train_size: int, seed: int
) -> DspyDatasetSplit:
    raw = load_dataset_raw(dataset_path)
    questions = collect_questions(raw)
    if train_size <= 0 or train_size >= len(questions):
        raise ValueError("train_size must be between 1 and dataset size - 1")
    groups: dict[str, list[EvalQuestion]] = {}
    for question in questions:
        difficulty = _difficulty_for_question(question)
        groups.setdefault(difficulty, []).append(question)
    if not groups:
        raise ValueError("Dataset did not contain any questions")

    group_sizes = {group: len(items) for group, items in groups.items()}
    allocation = _allocate_counts(group_sizes, train_size)
    rng = random.Random(seed)
    train: list[EvalQuestion] = []
    dev: list[EvalQuestion] = []
    for group in sorted(groups):
        items = groups[group]
        train_items, dev_items = _group_aware_partition(
            items,
            train_target=allocation[group],
            rng=rng,
        )
        train.extend(sorted(train_items, key=lambda q: q.id))
        dev.extend(sorted(dev_items, key=lambda q: q.id))
    if not train or not dev:
        raise ValueError("Group-aware split could not produce non-empty train/dev sets")
    train.sort(key=lambda q: q.id)
    dev.sort(key=lambda q: q.id)
    return DspyDatasetSplit(train=train, dev=dev)


def _group_aware_partition(
    items: list[EvalQuestion], *, train_target: int, rng: random.Random
) -> tuple[list[EvalQuestion], list[EvalQuestion]]:
    grouped_items: dict[str, list[EvalQuestion]] = {}
    for item in items:
        grouped_items.setdefault(_group_key_for_question(item), []).append(item)
    grouped_keys = list(grouped_items)
    rng.shuffle(grouped_keys)

    train: list[EvalQuestion] = []
    dev: list[EvalQuestion] = []
    remaining_keys = list(grouped_keys)
    remaining_items = sum(len(grouped_items[key]) for key in remaining_keys)
    train_remaining = train_target

    while remaining_keys:
        key = remaining_keys.pop(0)
        bucket = grouped_items[key]
        bucket_size = len(bucket)

        # Preserve at least one group for dev when possible.
        if not remaining_keys:
            if train_remaining >= bucket_size:
                train.extend(bucket)
                train_remaining = max(0, train_target - len(train))
            else:
                dev.extend(bucket)
            continue

        remaining_after = remaining_items - bucket_size
        would_overshoot = len(train) + bucket_size > train_target
        needs_more_train = train_remaining > 0
        can_still_hit_target_without = remaining_after >= train_remaining

        if needs_more_train and (not would_overshoot or not can_still_hit_target_without):
            train.extend(bucket)
            train_remaining = max(0, train_target - len(train))
        else:
            dev.extend(bucket)
        remaining_items = remaining_after

    return train, dev


def _group_key_for_question(question: EvalQuestion) -> str:
    if question.group_id:
        return question.group_id
    return question.id


def load_live_prompt_sections(
    mcp: StreamableHttpMcpClient, *, sample_question: str
) -> PromptSections:
    del sample_question
    payload = extract_json_content(
        mcp.call_tool("graph_schema", {"format": "structured"})
    )
    if not isinstance(payload, dict):
        raise ValueError("MCP graph_schema did not return a JSON object")
    sections = prompt_sections_from_graph_schema_payload(payload)
    if sections is None:
        raise ValueError("MCP graph_schema payload was missing schema sections")
    return sections


def build_examples(
    questions: Iterable[EvalQuestion], sections: PromptSections
) -> list[dspy.Example]:
    examples: list[dspy.Example] = []
    for question in questions:
        if question.expected is None:
            raise ValueError(f"Question {question.id} is missing expected rows")
        payload: dict[str, Any] = {
            "question_id": question.id,
            "difficulty": _difficulty_for_question(question),
            "user_question": question.question,
            "schema_reference": sections.schema_reference,
            "node_connectivity": sections.node_connectivity,
            "cypher": question.reference_cypher or "",
            "expected_columns": question.expected.columns,
            "expected_rows": question.expected.rows,
            "expected_ordered": question.expected.ordered,
        }
        if question.traversal_plan:
            payload["traversal_plan"] = question.traversal_plan
        examples.append(
            dspy.Example(**payload).with_inputs(
                "user_question", "schema_reference", "node_connectivity"
            )
        )
    return examples


def configure_lm(model: str, *, temperature: float | None = None) -> dspy.LM:
    provider = _detect_provider(model)
    base_url, api_key = _provider_credentials(provider)
    kwargs: dict[str, Any] = {
        "temperature": (
            temperature if temperature is not None else _temperature_for_model(model, provider)
        )
    }
    if api_key:
        kwargs["api_key"] = api_key
    if base_url:
        kwargs["api_base"] = base_url
    if provider in {"openai", "openai-compatible"}:
        lm_model = model
    elif provider == "gemini":
        normalized = model if "/" in model else f"gemini/{model}"
        lm_model = normalized
    else:
        lm_model = model
    return dspy.LM(lm_model, **kwargs)


def make_program(sections: PromptSections) -> dspy.Module:
    instruction = sections.tunable_instruction

    class AriadneCypherSignature(dspy.Signature):
        """placeholder"""

        user_question = dspy.InputField(
            desc="A natural-language SRE question about the Kubernetes graph."
        )
        schema_reference = dspy.InputField(
            desc="The authoritative graph schema and node property reference."
        )
        node_connectivity = dspy.InputField(
            desc="The authoritative directed graph relationships."
        )
        cypher = dspy.OutputField(
            desc="A valid Memgraph Cypher query only. Do not return JSON or explanations."
        )

    AriadneCypherSignature.__doc__ = instruction

    class AriadneCypherProgram(dspy.Module):
        def __init__(self) -> None:
            super().__init__()
            self.translate = dspy.Predict(AriadneCypherSignature)

        def forward(
            self, user_question: str, schema_reference: str, node_connectivity: str
        ) -> Any:
            return self.translate(
                user_question=user_question,
                schema_reference=schema_reference,
                node_connectivity=node_connectivity,
            )

    return AriadneCypherProgram()


def make_cot_program(sections: PromptSections) -> dspy.Module:
    """Multi-hop Chain-of-Thought program.

    Forces the model to plan the graph traversal explicitly before
    generating Cypher. The traversal_plan output captures the start
    node, target node, and the exact relationship chain, which DSPy
    can then optimize independently from the Cypher generation.
    """
    instruction = sections.tunable_instruction

    class AriadneCotCypherSignature(dspy.Signature):
        """placeholder"""

        user_question = dspy.InputField(
            desc="A natural-language SRE question about the Kubernetes graph."
        )
        schema_reference = dspy.InputField(
            desc="The authoritative graph schema and node property reference."
        )
        node_connectivity = dspy.InputField(
            desc="The authoritative directed graph relationships."
        )
        traversal_plan = dspy.OutputField(
            desc=(
                "Step-by-step graph traversal plan. "
                "Use one line per operation. "
                "Allowed line forms: "
                "'Entity', "
                "'EntityA -[:Relationship]-> EntityB', "
                "'OPTIONAL EntityA -[:Relationship]-> EntityB', "
                "'EXCLUDE EntityA -[:Relationship]-> EntityB', and "
                "'UNWIND Entity.path AS alias'. "
                "Use ONLY relationships from the node connectivity reference."
            )
        )
        cypher = dspy.OutputField(
            desc="A valid Memgraph Cypher query that follows the traversal plan above. Do not return JSON or explanations."
        )

    AriadneCotCypherSignature.__doc__ = instruction

    class AriadneCotCypherProgram(dspy.Module):
        def __init__(self) -> None:
            super().__init__()
            self.translate = dspy.Predict(AriadneCotCypherSignature)

        def forward(
            self, user_question: str, schema_reference: str, node_connectivity: str
        ) -> Any:
            return self.translate(
                user_question=user_question,
                schema_reference=schema_reference,
                node_connectivity=node_connectivity,
            )

    return AriadneCotCypherProgram()


def make_cot_program_v2(sections: PromptSections) -> dspy.Module:
    """Multi-hop CoT v2: schema/connectivity baked into instruction.

    Unlike v1, schema_reference and node_connectivity are part of the
    instruction text, not per-example input fields. This means DSPy
    demos only carry (user_question, traversal_plan, cypher) — no
    53k-char schema duplication per demo.
    """
    base_instruction = sections.tunable_instruction
    full_instruction = (
        f"{base_instruction}\n\n"
        f"## Graph Schema Reference\n{sections.schema_reference}\n\n"
        f"## Node Connectivity\n{sections.node_connectivity}"
    )

    class AriadneCotV2Signature(dspy.Signature):
        """placeholder"""

        user_question = dspy.InputField(
            desc="A natural-language SRE question about the Kubernetes graph."
        )
        traversal_plan = dspy.OutputField(
            desc=(
                "Step-by-step graph traversal plan. "
                "Use one line per operation. "
                "Allowed line forms: "
                "'Entity', "
                "'EntityA -[:Relationship]-> EntityB', "
                "'OPTIONAL EntityA -[:Relationship]-> EntityB', "
                "'EXCLUDE EntityA -[:Relationship]-> EntityB', and "
                "'UNWIND Entity.path AS alias'. "
                "Use ONLY relationships from the node connectivity in the instruction."
            )
        )
        cypher = dspy.OutputField(
            desc="A valid Memgraph Cypher query that follows the traversal plan above. Do not return JSON or explanations."
        )

    AriadneCotV2Signature.__doc__ = full_instruction

    class AriadneCotV2Program(dspy.Module):
        def __init__(self) -> None:
            super().__init__()
            self.translate = dspy.Predict(AriadneCotV2Signature)

        def forward(self, user_question: str, **_kwargs: Any) -> Any:
            return self.translate(user_question=user_question)

    return AriadneCotV2Program()


def build_cot_v2_examples(
    questions: Iterable[EvalQuestion], sections: PromptSections
) -> list[dspy.Example]:
    """Build examples for CoT v2 — only user_question as input.

    schema_reference and node_connectivity are NOT per-example fields
    since they're baked into the instruction.
    """
    examples: list[dspy.Example] = []
    for question in questions:
        if question.expected is None:
            raise ValueError(f"Question {question.id} is missing expected rows")
        payload: dict[str, Any] = {
            "question_id": question.id,
            "difficulty": _difficulty_for_question(question),
            "user_question": question.question,
            "cypher": question.reference_cypher or "",
            "expected_columns": question.expected.columns,
            "expected_rows": question.expected.rows,
            "expected_ordered": question.expected.ordered,
        }
        if question.traversal_plan:
            payload["traversal_plan"] = question.traversal_plan
        examples.append(
            dspy.Example(**payload).with_inputs("user_question")
        )
    return examples


class CypherExecutionMetric:
    def __init__(self, graph: GraphMcpClient):
        self._graph = graph

    def __call__(self, example: dspy.Example, pred: Any, trace: Any = None) -> float:
        cypher = _clean_cypher(getattr(pred, "cypher", ""))
        if not cypher:
            return 0.0
        try:
            result = self._graph.execute_cypher(cypher)
        except Exception:
            return 0.0
        expected = ExpectedResult(
            columns=list(example.expected_columns),
            rows=list(example.expected_rows),
            ordered=bool(example.expected_ordered),
        )
        evaluation = evaluate_expected_match(result, expected)
        return _score_match_evaluation(evaluation)


def _score_match_evaluation(evaluation: MatchEvaluation) -> float:
    """Map the taxonomy-aware match evaluation to a continuous [0, 1] score.

    The goal is to give the MIPROv2 optimizer a gradient-like signal so it can
    distinguish near-misses from total failures, rewarding demos and instructions
    that move the student closer to correctness.
    """
    mt = evaluation.match_type
    if mt in {MatchType.EXACT, MatchType.PROJECTED}:
        return 1.0

    overlap_ratio = _overlap_ratio(evaluation)

    if mt is MatchType.ORDERING_MISMATCH:
        # All rows present, just wrong order — very close.
        return 0.9

    if mt is MatchType.EXTRA_ROWS:
        # All expected rows present but query is over-inclusive.
        # Scale: 0.3 (terrible precision) to 0.7 (few extras).
        # A query returning 1000 rows when 10 expected should score well below
        # MISSING_ROWS with high overlap.
        if evaluation.result_count and evaluation.expected_count > 0:
            precision = min(evaluation.expected_count / evaluation.result_count, 1.0)
            return 0.3 + 0.4 * precision
        return 0.4

    if mt is MatchType.MISSING_ROWS:
        # Partial overlap — scale linearly in [0.2, 0.7].
        return 0.2 + 0.5 * overlap_ratio

    if mt is MatchType.EXTRA_AND_MISSING:
        # Some right, some wrong, some missing — worse than pure missing.
        return 0.15 + 0.35 * overlap_ratio

    if mt is MatchType.GROUPED:
        # Wrong shape (aggregated/nested) — small credit for trying.
        return 0.1 + 0.15 * overlap_ratio

    if mt is MatchType.INSUFFICIENT_COLUMNS:
        return 0.1

    if mt is MatchType.EMPTY_RESULT:
        return 0.05

    if mt is MatchType.WRONG_SEMANTICS:
        # Executed successfully but completely wrong data.
        return 0.05

    # NON_TABULAR, INVALID, EXECUTION_ERROR
    return 0.0


def _overlap_ratio(evaluation: MatchEvaluation) -> float:
    if evaluation.best_overlap is None or evaluation.expected_count <= 0:
        return 0.0
    return min(evaluation.best_overlap / evaluation.expected_count, 1.0)


def evaluate_program(
    program: dspy.Module,
    examples: Iterable[dspy.Example],
    *,
    graph: GraphMcpClient,
    parallelism: int = 1,
) -> ModelEvalResult:
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    example_list = list(examples)

    def _eval_one(example: dspy.Example) -> dict[str, Any]:
        start = time.perf_counter()
        # Build kwargs from the example's input fields.
        # cot-v2 only has user_question; predict/cot have all three.
        call_kwargs: dict[str, str] = {"user_question": example.user_question}
        if hasattr(example, "schema_reference"):
            call_kwargs["schema_reference"] = example.schema_reference
        if hasattr(example, "node_connectivity"):
            call_kwargs["node_connectivity"] = example.node_connectivity
        pred = program(**call_kwargs)
        latency_ms = (time.perf_counter() - start) * 1000.0
        cypher = _clean_cypher(getattr(pred, "cypher", ""))
        row: dict[str, Any] = {
            "question_id": example.question_id,
            "difficulty": getattr(example, "difficulty", None),
            "latency_ms": latency_ms,
            "cypher": cypher,
            "valid": False,
            "execution_error": None,
            "result_match": False,
        }
        if not cypher:
            row["match_type"] = MatchType.INVALID.value
            return row
        row["valid"] = True
        expected = ExpectedResult(
            columns=list(example.expected_columns),
            rows=list(example.expected_rows),
            ordered=bool(example.expected_ordered),
        )
        try:
            result = graph.execute_cypher(cypher)
            row["row_count"] = len(result) if isinstance(result, list) else None
            match_eval = evaluate_expected_match(result, expected)
            row["result_match"] = match_eval.matched
            row["match_type"] = match_eval.match_type.value
            row["match_details"] = match_eval.as_dict()
        except Exception as exc:
            row["execution_error"] = str(exc)
            row["match_type"] = MatchType.EXECUTION_ERROR.value
        return row

    if parallelism <= 1:
        rows = [_eval_one(ex) for ex in example_list]
    else:
        rows = [None] * len(example_list)  # type: ignore[list-item]
        with ThreadPoolExecutor(max_workers=parallelism) as pool:
            future_to_idx = {
                pool.submit(_eval_one, ex): i
                for i, ex in enumerate(example_list)
            }
            for future in as_completed(future_to_idx):
                rows[future_to_idx[future]] = future.result()

    total = 0
    valid = 0
    exec_error = 0
    exact_match = 0
    invalid = 0
    exact_column_match = 0
    projected_match = 0
    match_type_counts: Counter[str] = Counter()
    total_latency_ms = 0.0
    rows_by_question: dict[str, dict[str, Any]] = {}
    for row in rows:
        total += 1
        total_latency_ms += row["latency_ms"]
        mt = row.get("match_type", MatchType.INVALID.value)
        match_type_counts[mt] += 1
        if not row["valid"]:
            invalid += 1
        else:
            valid += 1
            if row.get("execution_error"):
                exec_error += 1
            elif row.get("result_match"):
                exact_match += 1
                if mt == MatchType.EXACT.value:
                    exact_column_match += 1
                elif mt == MatchType.PROJECTED.value:
                    projected_match += 1
        rows_by_question[row["question_id"]] = row
    counts = EvalCounts(
        total=total,
        valid=valid,
        exec_error=exec_error,
        exact_match=exact_match,
        invalid=invalid,
        exact_column_match=exact_column_match,
        projected_match=projected_match,
        match_type_counts=dict(match_type_counts),
    )
    avg_latency_ms = total_latency_ms / total if total else 0.0
    return ModelEvalResult(
        counts=counts,
        avg_latency_ms=avg_latency_ms,
        rows_by_question=rows_by_question,
    )


def save_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _difficulty_for_question(question: EvalQuestion) -> str:
    for tag in question.tags:
        if tag.startswith("difficulty:"):
            return tag.split(":", 1)[1]
    prefix = question.id[:1].lower()
    return {"e": "easy", "m": "medium", "h": "hard"}.get(prefix, "unknown")


def _allocate_counts(group_sizes: Mapping[str, int], train_size: int) -> dict[str, int]:
    names = sorted(group_sizes)
    total = sum(group_sizes.values())
    allocation = {name: 0 for name in names}
    remainders: list[tuple[float, str]] = []
    assigned = 0
    for name in names:
        exact = train_size * group_sizes[name] / total
        base = min(group_sizes[name], int(exact))
        allocation[name] = base
        assigned += base
        remainders.append((exact - base, name))
    remaining = train_size - assigned
    for _fraction, name in sorted(remainders, reverse=True):
        if remaining <= 0:
            break
        if allocation[name] >= group_sizes[name]:
            continue
        allocation[name] += 1
        remaining -= 1
    return allocation


def _clean_cypher(value: str | None) -> str:
    if not value:
        return ""
    normalized = value.strip()
    if normalized.startswith("```"):
        lines = normalized.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        normalized = "\n".join(lines).strip()
    return normalized

def _detect_provider(model: str) -> str | None:
    lowered = model.strip().lower()
    if "/" in lowered:
        prefix = lowered.split("/", 1)[0]
        if prefix in {"openai", "gemini", "google"}:
            return "gemini" if prefix in {"gemini", "google"} else prefix
    if lowered.startswith("gemini"):
        return "gemini"
    if lowered.startswith(("gpt", "o1", "o3", "o4")):
        return "openai"
    return None


def _provider_credentials(provider: str | None) -> tuple[str | None, str | None]:
    if provider in {"openai", "openai-compatible"}:
        return os.environ.get("OPENAI_BASE_URL"), os.environ.get("OPENAI_API_KEY")
    if provider == "gemini":
        return (
            os.environ.get("GOOGLE_GEMINI_BASE_URL"),
            os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"),
        )
    return os.environ.get("LLM_BASE_URL"), None


def _temperature_for_model(model: str, provider: str | None) -> float:
    normalized = model.lower()
    if "/" in normalized:
        normalized = normalized.split("/", 1)[1]
    if provider == "openai" and normalized.startswith("gpt-5"):
        return 1.0
    return 0.2


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _match_expected(result: Any, expected: ExpectedResult) -> bool:
    return match_expected(result, expected)
