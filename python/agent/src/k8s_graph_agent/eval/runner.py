from __future__ import annotations

from dataclasses import asdict, dataclass, replace
from datetime import datetime
import json
import logging
import os
from pathlib import Path
import threading
import time
from typing import Any, Iterable, Mapping, cast
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

from ..adk_translate import (
    AdkCypherTranslator,
    TokenUsage,
    TranslationAttempt,
    TranslationOutcome,
)
from ..agent import GraphMcpClient
from ..config import AdkConfig, AgentConfig
from ..mcp_client import JsonRpcError, StreamableHttpMcpClient
from ..models import JsonValue
from ..logging_utils import format_java_like
from .loader import load_dataset
from .matching import MatchType, evaluate_expected_match
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


@dataclass(frozen=True)
class QueryFailureClassification:
    match_type: str
    error_field: str
    error_message: str
    issue_kind: str | None = None
    issue_source: str | None = None


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
                        logger.error(
                            "evaluation failed for question %s\n%s",
                            question.id,
                            format_java_like(exc),
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
        logger.error(
            "evaluation failed for question %s\n%s",
            question.id,
            format_java_like(exc),
        )
        return _error_record(
            model=model,
            question_id=question.id,
            run_index=run_index,
            mode=mode,
            error=str(exc),
            elapsed_ms=elapsed_ms,
        )
    elapsed_ms = int((time.perf_counter() - start) * 1000)

    final_payload: dict[str, Any] = {
        "valid": outcome.cypher is not None,
        "error": outcome.error,
        "cypher": outcome.cypher,
    }
    result_match: bool | None = None
    execution_error: str | None = None
    query_failure: QueryFailureClassification | None = None
    attempts = list(outcome.attempts)
    total_usage = TokenUsage()
    total_usage.add(outcome.total_usage)
    if outcome.cypher:
        try:
            result = graph.execute_cypher(outcome.cypher)
            if question.expected:
                match_eval = evaluate_expected_match(result, question.expected)
                result_match = match_eval.matched
                final_payload["match_type"] = match_eval.match_type.value
                final_payload["match_details"] = match_eval.as_dict()
            final_payload["rows"] = _count_rows(result)
        except Exception as exc:
            execution_error = str(exc)
            repaired = _attempt_execution_repair(
                translator=translator,
                graph=graph,
                question=question,
                mode=mode,
                original_cypher=outcome.cypher,
                execution_error=execution_error,
            )
            if repaired is not None:
                repair_outcome, result = repaired
                attempts.extend(repair_outcome.attempts)
                total_usage.add(repair_outcome.total_usage)
                final_payload.update(
                    {
                        "valid": repair_outcome.cypher is not None,
                        "error": repair_outcome.error,
                        "cypher": repair_outcome.cypher,
                    }
                )
                execution_error = None
                if question.expected:
                    match_eval = evaluate_expected_match(result, question.expected)
                    result_match = match_eval.matched
                    final_payload["match_type"] = match_eval.match_type.value
                    final_payload["match_details"] = match_eval.as_dict()
                final_payload["rows"] = _count_rows(result)
            else:
                query_failure = _classify_query_failure(exc)
    if execution_error:
        assert query_failure is not None
        final_payload[query_failure.error_field] = query_failure.error_message
        if query_failure.issue_kind is not None:
            final_payload["query_issue_kind"] = query_failure.issue_kind
        if query_failure.issue_source is not None:
            final_payload["query_issue_source"] = query_failure.issue_source
        final_payload["match_type"] = query_failure.match_type
    if question.expected is not None:
        final_payload["result_match"] = result_match
        if "match_type" not in final_payload:
            final_payload["match_type"] = (
                MatchType.INVALID.value if not outcome.cypher else None
            )

    metrics = {
        "attempts": len(attempts),
        "latency_ms": elapsed_ms,
        "total_tokens": total_usage.total_tokens,
        "total_prompt_tokens": total_usage.prompt_tokens,
        "total_output_tokens": total_usage.output_tokens,
    }

    return EvalRecord(
        model=model,
        question_id=question.id,
        run_index=run_index,
        mode=mode,
        attempts=[_attempt_payload(a) for a in attempts],
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
    except Exception as exc:
        logger.debug("failed to close MCP client\n%s", format_java_like(exc))


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
        log_dir_str = cast(str, log_dir)
        path = Path(log_dir_str) / f"k8s-graph-eval-{timestamp}-pid{os.getpid()}.log"
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
        thread_name = args.thread.name if args.thread is not None else "<unknown>"
        exc_type = args.exc_type or RuntimeError
        exc_value = args.exc_value or RuntimeError("thread exception without value")
        message = format_java_like(exc_value, thread_name=thread_name)
        logger.error(message)

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


def _attempt_execution_repair(
    translator: AdkCypherTranslator,
    graph: GraphMcpClient,
    question: EvalQuestion,
    mode: str,
    original_cypher: str,
    execution_error: str,
) -> tuple[TranslationOutcome, JsonValue] | None:
    if mode != "retry":
        return None
    retry_fn = getattr(translator, "translate_with_execution_error", None)
    if not callable(retry_fn):
        return None
    logger.warning(
        "execution failed for %s; attempting repair: %s",
        question.id,
        execution_error,
    )
    try:
        repair_outcome = retry_fn(
            question.question,
            original_cypher,
            execution_error,
            max_attempts=1,
        )
    except Exception as exc:
        logger.warning(
            "execution-guided repair failed for %s\n%s",
            question.id,
            format_java_like(exc),
        )
        return None
    if repair_outcome.cypher is None:
        return None
    try:
        result = graph.execute_cypher(repair_outcome.cypher)
    except Exception:
        return None
    return repair_outcome, result


def _count_rows(result: JsonValue) -> int | None:
    if isinstance(result, list):
        return len(result)
    return None


def _classify_query_failure(exc: Exception) -> QueryFailureClassification:
    issue_kind: str | None = None
    issue_source: str | None = None
    if isinstance(exc, JsonRpcError):
        data = exc.error.get("data")
        if isinstance(data, dict):
            raw_kind = data.get("kind")
            raw_source = data.get("source")
            if isinstance(raw_kind, str) and raw_kind:
                issue_kind = raw_kind
            if isinstance(raw_source, str) and raw_source:
                issue_source = raw_source
        if issue_source == "validator":
            return QueryFailureClassification(
                match_type=_validator_match_type(issue_kind),
                error_field="validator_error",
                error_message=str(exc),
                issue_kind=issue_kind,
                issue_source=issue_source,
            )
    return QueryFailureClassification(
        match_type=MatchType.EXECUTION_ERROR.value,
        error_field="execution_error",
        error_message=str(exc),
        issue_kind=issue_kind,
        issue_source=issue_source,
    )


def _validator_match_type(issue_kind: str | None) -> str:
    if issue_kind is None:
        return "validator_error"
    if issue_kind.startswith("validator_"):
        return issue_kind
    return f"validator_{issue_kind}"
