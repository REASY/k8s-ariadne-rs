from __future__ import annotations

import argparse
from dataclasses import replace
import json
import os
from pathlib import Path
from typing import Any

from k8s_graph_agent.adk_translate import AdkCypherTranslator
from k8s_graph_agent.agent import GraphMcpClient
from k8s_graph_agent.config import AdkConfig, AgentConfig
from k8s_graph_agent.eval.bootstrap import (
    consensus_fingerprint,
    load_dataset_raw,
    result_fingerprint,
    select_questions,
)
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate model-assisted reference Cypher candidates for eval questions."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions.yaml"),
        help="Dataset path (default: eval/questions.yaml).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("eval/gold_candidates.json"),
        help="Output JSON path (default: eval/gold_candidates.json).",
    )
    parser.add_argument(
        "--models",
        required=True,
        help="Comma-separated candidate models to run.",
    )
    parser.add_argument(
        "--ids",
        help="Comma-separated question ids to include.",
    )
    parser.add_argument(
        "--tags",
        help="Comma-separated tags to include.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional limit after filtering.",
    )
    parser.add_argument(
        "--attempts",
        type=int,
        default=2,
        help="Max translation attempts per model/question (default: 2).",
    )
    parser.add_argument(
        "--include-nondeterministic",
        action="store_true",
        help="Include questions without deterministic=true.",
    )
    args = parser.parse_args()

    raw = load_dataset_raw(args.dataset)
    question_ids = _split_csv(args.ids)
    tags = _split_csv(args.tags)
    questions = select_questions(
        raw,
        ids=question_ids,
        tags=tags,
        deterministic_only=not args.include_nondeterministic,
    )
    if args.limit is not None:
        questions = questions[: max(0, args.limit)]
    if not questions:
        raise SystemExit("No questions matched the provided filters.")

    models = [value for value in (part.strip() for part in args.models.split(",")) if value]
    if not models:
        raise SystemExit("No models provided.")

    agent_config = AgentConfig.from_env()
    base_adk_config = AdkConfig.from_env()
    output_payload: list[dict[str, Any]] = []
    for question in questions:
        print(f"[candidate] question={question.id}")
        model_runs: list[dict[str, Any]] = []
        for model in models:
            try:
                run = _run_candidate(
                    question_id=question.id,
                    question_text=question.question,
                    model=model,
                    agent_config=agent_config,
                    base_adk_config=base_adk_config,
                    max_attempts=args.attempts,
                )
            except Exception as exc:
                print(f"  model={model} failed before completion: {exc}")
                run = {
                    "question_id": question.id,
                    "model": model,
                    "provider": _detect_provider(model),
                    "valid": False,
                    "error": str(exc),
                    "cypher": None,
                    "attempts": [],
                    "execution_error": None,
                    "row_count": None,
                    "result_fingerprint": None,
                }
            model_runs.append(run)
        consensus = consensus_fingerprint(run.get("result_fingerprint") for run in model_runs)
        output_payload.append(
            {
                "id": question.id,
                "question": question.question,
                "tags": question.tags,
                "deterministic": question.deterministic,
                "reference_cypher": question.reference_cypher,
                "consensus_result_fingerprint": consensus,
                "consensus_models": [
                    run["model"]
                    for run in model_runs
                    if run.get("result_fingerprint") == consensus and consensus is not None
                ],
                "candidates": model_runs,
            }
        )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output_payload, indent=2), encoding="utf-8")
    print(f"Wrote {len(output_payload)} question records -> {args.output}")


def _run_candidate(
    *,
    question_id: str,
    question_text: str,
    model: str,
    agent_config: AgentConfig,
    base_adk_config: AdkConfig,
    max_attempts: int,
) -> dict[str, Any]:
    provider = _detect_provider(model)
    config = _config_for_model(base_adk_config, model=model, provider=provider)
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    try:
        translator = AdkCypherTranslator(mcp=mcp, config=config)
        graph = GraphMcpClient(mcp=mcp)
        outcome = translator.translate_with_attempts(question_text, max_attempts=max_attempts)
        execution_error: str | None = None
        rows: int | None = None
        fingerprint: str | None = None
        if outcome.cypher:
            try:
                result = graph.execute_cypher(outcome.cypher)
                rows = len(result) if isinstance(result, list) else 1
                fingerprint = result_fingerprint(result)
            except Exception as exc:
                execution_error = str(exc)
        print(
            f"  model={model} valid={outcome.cypher is not None} "
            f"exec_error={'yes' if execution_error else 'no'}"
        )
        return {
            "question_id": question_id,
            "model": model,
            "provider": provider,
            "valid": outcome.cypher is not None,
            "error": outcome.error,
            "cypher": outcome.cypher,
            "attempts": [
                {
                    "attempt": attempt.attempt,
                    "valid": attempt.valid,
                    "error": attempt.error,
                    "cypher": attempt.cypher,
                    "tokens": {
                        "prompt": attempt.usage.prompt_tokens,
                        "output": attempt.usage.output_tokens,
                        "total": attempt.usage.total_tokens,
                    },
                }
                for attempt in outcome.attempts
            ],
            "execution_error": execution_error,
            "row_count": rows,
            "result_fingerprint": fingerprint,
        }
    finally:
        mcp.close()


def _split_csv(raw: str | None) -> set[str] | None:
    if raw is None:
        return None
    values = {part.strip() for part in raw.split(",") if part.strip()}
    return values or None


def _config_for_model(
    base_adk_config: AdkConfig, *, model: str, provider: str | None
) -> AdkConfig:
    base_url, api_key = _provider_credentials(provider)
    temperature = (
        1.0
        if (provider in {"openai", "openai-compatible"} and _strip_provider_prefix(model).startswith("gpt-5"))
        else base_adk_config.temperature
    )
    return replace(
        base_adk_config,
        model=model,
        provider=provider,
        base_url=base_url,
        api_key=api_key,
        temperature=temperature,
    )


def _provider_credentials(provider: str | None) -> tuple[str | None, str | None]:
    if provider in {"openai", "openai-compatible"}:
        return (
            os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL"),
            os.environ.get("OPENAI_API_KEY"),
        )
    if provider == "gemini":
        return (
            os.environ.get("GOOGLE_GEMINI_BASE_URL"),
            os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY"),
        )
    if provider == "anthropic":
        return (
            os.environ.get("ANTHROPIC_BASE_URL") or os.environ.get("CLAUDE_BASE_URL"),
            os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY"),
        )
    if provider == "deepseek":
        return (
            os.environ.get("DEEPSEEK_BASE_URL"),
            os.environ.get("DEEPSEEK_API_KEY"),
        )
    return base_url_from_env(), None


def base_url_from_env() -> str | None:
    return os.environ.get("LLM_BASE_URL")


def _strip_provider_prefix(model: str) -> str:
    normalized = model.strip()
    if "/" in normalized:
        return normalized.split("/", 1)[1]
    return normalized


def _detect_provider(model: str) -> str | None:
    lowered = model.strip().lower()
    if "/" in lowered:
        prefix = lowered.split("/", 1)[0]
        if prefix in {"openai", "gemini", "google", "anthropic", "claude", "deepseek"}:
            if prefix in {"gemini", "google"}:
                return "gemini"
            if prefix in {"claude", "anthropic"}:
                return "anthropic"
            return prefix
    if lowered.startswith("gemini"):
        return "gemini"
    if lowered.startswith("claude"):
        return "anthropic"
    if lowered.startswith("deepseek"):
        return "deepseek"
    if lowered.startswith(("gpt", "o1", "o3", "o4")):
        return "openai"
    return None


if __name__ == "__main__":
    main()
