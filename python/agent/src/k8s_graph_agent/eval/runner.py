from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import logging
from pathlib import Path
import time
from typing import Any, Iterable, Mapping

from ..adk_translate import AdkCypherTranslator, TranslationOutcome, TranslationAttempt
from ..agent import GraphMcpClient
from ..config import AdkConfig, AgentConfig
from ..mcp_client import StreamableHttpMcpClient
from ..models import JsonValue
from .loader import load_dataset
from .models import EvalQuestion, ExpectedResult

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EvalRecord:
    model: str
    question_id: str
    run_index: int
    mode: str
    attempts: list[dict[str, Any]]
    final: dict[str, Any]
    metrics: dict[str, Any]


def run_evaluation(
    dataset_path: Path,
    mode: str,
    runs: int,
    output_path: Path | None = None,
) -> list[EvalRecord]:
    if mode not in {"single-shot", "retry"}:
        raise ValueError(f"Unsupported mode: {mode}")
    questions = load_dataset(dataset_path)
    agent_config = AgentConfig.from_env()
    adk_config = AdkConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    translator = AdkCypherTranslator(mcp=mcp, config=adk_config)
    graph = GraphMcpClient(mcp=mcp)

    records: list[EvalRecord] = []
    output_handle = None
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_handle = output_path.open("a", encoding="utf-8")
    total = len(questions) * runs
    counter = 0
    try:
        for run_index in range(1, runs + 1):
            for question in questions:
                counter += 1
                logger.info(
                    "[%d/%d] run %d/%d question %s",
                    counter,
                    total,
                    run_index,
                    runs,
                    question.id,
                )
                record = _run_question(
                    translator=translator,
                    graph=graph,
                    question=question,
                    mode=mode,
                    run_index=run_index,
                    model=adk_config.model,
                )
                records.append(record)
                payload = json.dumps(asdict(record), default=str)
                if output_handle is None:
                    print(payload)
                else:
                    output_handle.write(payload + "\n")
                    output_handle.flush()
    finally:
        if output_handle is not None:
            output_handle.close()
    return records


def _run_question(
    translator: AdkCypherTranslator,
    graph: GraphMcpClient,
    question: EvalQuestion,
    mode: str,
    run_index: int,
    model: str,
) -> EvalRecord:
    max_attempts = 1 if mode == "single-shot" else 2
    start = time.perf_counter()
    try:
        outcome = translator.translate_with_attempts(
            question.question, max_attempts=max_attempts
        )
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        logger.exception("evaluation failed for question %s", question.id)
        return EvalRecord(
            model=model,
            question_id=question.id,
            run_index=run_index,
            mode=mode,
            attempts=[],
            final={"valid": False, "error": str(exc), "cypher": None},
            metrics={
                "attempts": 0,
                "latency_ms": elapsed_ms,
                "total_tokens": None,
                "total_prompt_tokens": None,
                "total_output_tokens": None,
            },
        )
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    attempts_payload = [_attempt_payload(a) for a in outcome.attempts]
    final_payload: dict[str, Any] = {
        "valid": outcome.cypher is not None,
        "error": outcome.error,
        "cypher": outcome.cypher,
    }
    result_match: bool | None = None
    execution_error: str | None = None
    if outcome.cypher:
        try:
            result = graph.execute_cypher(outcome.cypher)
            if question.expected:
                result_match = _match_expected(result, question.expected)
            final_payload["rows"] = _count_rows(result)
        except Exception as exc:
            execution_error = str(exc)
    if execution_error:
        final_payload["execution_error"] = execution_error
    if question.expected is not None:
        final_payload["result_match"] = result_match

    metrics = {
        "attempts": len(outcome.attempts),
        "latency_ms": elapsed_ms,
        "total_tokens": outcome.total_usage.total_tokens,
        "total_prompt_tokens": outcome.total_usage.prompt_tokens,
        "total_output_tokens": outcome.total_usage.output_tokens,
    }

    return EvalRecord(
        model=model,
        question_id=question.id,
        run_index=run_index,
        mode=mode,
        attempts=attempts_payload,
        final=final_payload,
        metrics=metrics,
    )


def _attempt_payload(attempt: TranslationAttempt) -> dict[str, Any]:
    usage = attempt.usage
    return {
        "attempt": attempt.attempt,
        "valid": attempt.valid,
        "error": attempt.error,
        "cypher": attempt.cypher,
        "tokens": {
            "prompt": usage.prompt_tokens,
            "output": usage.output_tokens,
            "total": usage.total_tokens,
        },
    }


def _match_expected(result: JsonValue, expected: ExpectedResult) -> bool:
    if not isinstance(result, list):
        return False
    normalized = _normalize_rows(result, expected.columns)
    if normalized is None:
        return False
    expected_rows = [tuple(row) for row in expected.rows]
    if expected.ordered:
        return normalized == expected_rows
    return _multiset_equal(normalized, expected_rows)


def _normalize_rows(
    rows: Iterable[Mapping[str, Any]], columns: list[str]
) -> list[tuple[Any, ...]] | None:
    normalized: list[tuple[Any, ...]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return None
        if any(col not in row for col in columns):
            return None
        normalized.append(tuple(row[col] for col in columns))
    return normalized


def _multiset_equal(left: list[tuple[Any, ...]], right: list[tuple[Any, ...]]) -> bool:
    from collections import Counter

    return Counter(left) == Counter(right)


def _count_rows(result: JsonValue) -> int | None:
    if isinstance(result, list):
        return len(result)
    return None
