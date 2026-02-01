from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Iterable, Mapping
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..adk_translate import AdkCypherTranslator, TranslationOutcome, TranslationAttempt
from ..agent import GraphMcpClient
from ..config import AdkConfig, AgentConfig
from ..mcp_client import StreamableHttpMcpClient
from ..models import JsonValue
from .loader import load_dataset
from .models import EvalQuestion, ExpectedResult

logger = logging.getLogger(__name__)
_FILE_LOGGING_CONFIGURED = False


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
    _configure_file_logging()
    if mode not in {"single-shot", "retry"}:
        raise ValueError(f"Unsupported mode: {mode}")
    questions = load_dataset(dataset_path)
    agent_config = AgentConfig.from_env()
    adk_config = AdkConfig.from_env()
    parallelism = _eval_parallelism()
    if parallelism > 1:
        logger.info("running eval with parallelism=%d", parallelism)
    else:
        logger.info("running eval with parallelism=1")

    records: list[EvalRecord] = []
    output_handle = None
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_handle = output_path.open("a", encoding="utf-8")
    total = len(questions) * runs
    counter = 0
    try:
        if parallelism <= 1:
            translator, graph, mcp = _build_clients(agent_config, adk_config)
            try:
                for run_index in range(1, runs + 1):
                    for question in questions:
                        counter += 1
                        record = _run_question(
                            translator=translator,
                            graph=graph,
                            question=question,
                            mode=mode,
                            run_index=run_index,
                            model=adk_config.model,
                            counter=counter,
                            total=total,
                            runs=runs,
                        )
                        records.append(record)
                        _emit_record(output_handle, record)
            finally:
                _close_mcp(mcp)
        else:
            tasks: list[tuple[int, int, EvalQuestion]] = []
            for run_index in range(1, runs + 1):
                for question in questions:
                    counter += 1
                    tasks.append((counter, run_index, question))
            with ThreadPoolExecutor(max_workers=parallelism) as executor:
                future_map = {
                    executor.submit(
                        _run_question_parallel,
                        agent_config,
                        adk_config,
                        question,
                        mode,
                        run_index,
                        runs,
                        counter,
                        total,
                    ): (counter, run_index, question)
                    for counter, run_index, question in tasks
                }
                for future in as_completed(future_map):
                    counter, run_index, question = future_map[future]
                    try:
                        record = future.result()
                    except Exception as exc:  # pragma: no cover
                        logger.exception(
                            "evaluation failed for question %s", question.id
                        )
                        record = _error_record(
                            model=adk_config.model,
                            question_id=question.id,
                            run_index=run_index,
                            mode=mode,
                            error=str(exc),
                            elapsed_ms=0,
                        )
                    records.append(record)
                    _emit_record(output_handle, record)
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
    counter: int | None = None,
    total: int | None = None,
    runs: int | None = None,
) -> EvalRecord:
    if counter is not None and total is not None and runs is not None:
        logger.info(
            "[%d/%d] run %d/%d question %s",
            counter,
            total,
            run_index,
            runs,
            question.id,
        )
    max_attempts = 1 if mode == "single-shot" else 2
    start = time.perf_counter()
    try:
        outcome = translator.translate_with_attempts(
            question.question, max_attempts=max_attempts
        )
    except Exception as exc:
        elapsed_ms = int((time.perf_counter() - start) * 1000)
        logger.exception("evaluation failed for question %s", question.id)
        return _error_record(
            model=model,
            question_id=question.id,
            run_index=run_index,
            mode=mode,
            error=str(exc),
            elapsed_ms=elapsed_ms,
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


def _run_question_parallel(
    agent_config: AgentConfig,
    adk_config: AdkConfig,
    question: EvalQuestion,
    mode: str,
    run_index: int,
    runs: int,
    counter: int,
    total: int,
) -> EvalRecord:
    session_suffix = f"r{run_index}-q{question.id}-{uuid.uuid4().hex[:8]}"
    translator, graph, mcp = _build_clients(
        agent_config, adk_config, session_suffix=session_suffix
    )
    try:
        return _run_question(
            translator=translator,
            graph=graph,
            question=question,
            mode=mode,
            run_index=run_index,
            model=adk_config.model,
            counter=counter,
            total=total,
            runs=runs,
        )
    finally:
        _close_mcp(mcp)


def _build_clients(
    agent_config: AgentConfig,
    adk_config: AdkConfig,
    session_suffix: str | None = None,
) -> tuple[AdkCypherTranslator, GraphMcpClient, StreamableHttpMcpClient]:
    if session_suffix:
        adk_config = replace(
            adk_config, session_id=f"{adk_config.session_id}-{session_suffix}"
        )
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    translator = AdkCypherTranslator(mcp=mcp, config=adk_config)
    graph = GraphMcpClient(mcp=mcp)
    return translator, graph, mcp


def _close_mcp(mcp: StreamableHttpMcpClient) -> None:
    try:
        mcp.close()
    except Exception:
        logger.debug("failed to close MCP client", exc_info=True)


def _emit_record(output_handle: Any, record: EvalRecord) -> None:
    payload = json.dumps(asdict(record), default=str)
    if output_handle is None:
        print(payload)
    else:
        output_handle.write(payload + "\n")
        output_handle.flush()


def _error_record(
    model: str,
    question_id: str,
    run_index: int,
    mode: str,
    error: str,
    elapsed_ms: int,
) -> EvalRecord:
    return EvalRecord(
        model=model,
        question_id=question_id,
        run_index=run_index,
        mode=mode,
        attempts=[],
        final={"valid": False, "error": error, "cypher": None},
        metrics={
            "attempts": 0,
            "latency_ms": elapsed_ms,
            "total_tokens": None,
            "total_prompt_tokens": None,
            "total_output_tokens": None,
        },
    )


def _eval_parallelism() -> int:
    raw = os.environ.get("K8S_GRAPH_EVAL_PARALLELISM") or os.environ.get(
        "EVAL_PARALLELISM", "1"
    )
    try:
        value = int(raw)
    except ValueError:
        return 1
    return max(1, value)


def _configure_file_logging() -> None:
    global _FILE_LOGGING_CONFIGURED
    if _FILE_LOGGING_CONFIGURED:
        return
    log_file = os.environ.get("K8S_GRAPH_LOG_FILE")
    log_dir = os.environ.get("K8S_GRAPH_LOG_DIR")
    if not log_file and not log_dir:
        return
    if log_file:
        path = Path(log_file)
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = Path(log_dir) / f"k8s-graph-eval-{timestamp}-pid{os.getpid()}.log"
    path.parent.mkdir(parents=True, exist_ok=True)

    handler = logging.FileHandler(path, encoding="utf-8")
    level_name = os.environ.get("K8S_GRAPH_LOG_FILE_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.DEBUG)
    handler.setLevel(level)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s [%(threadName)s]: %(message)s"
        )
    )

    root = logging.getLogger()
    root.addHandler(handler)
    _install_thread_excepthook()
    _FILE_LOGGING_CONFIGURED = True
    logger.info("file logging enabled at %s", path)


def _install_thread_excepthook() -> None:
    def _hook(args: threading.ExceptHookArgs) -> None:
        logger.error(
            "unhandled exception in thread %s",
            args.thread.name,
            exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
        )

    threading.excepthook = _hook


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
