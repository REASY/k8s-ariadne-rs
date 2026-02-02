from __future__ import annotations

import argparse
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime
import json
import os
from pathlib import Path
import re
from typing import Iterator
import sys
import time
from multiprocessing import get_context
from multiprocessing.process import BaseProcess

from k8s_graph_agent.eval.runner import run_evaluation


DEFAULT_MODELS = [
    "claude-sonnet-4-20250514",
    "claude-sonnet-4-5-20250929",
    "claude-haiku-4-5-20251001",
    "claude-opus-4-5-20251101",
    "deepseek-r1",
    "openai/gpt-5-mini-2025-08-07",
    "openai/gpt-5-nano-2025-08-07",
    "openai/gpt-5.2-2025-12-11",
    "gemini-2.5-flash-lite",
    "gemini-2.5-flash",
    "gemini-3-pro-preview",
    "gemini-3-flash-preview",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run NL → Cypher evals across multiple models."
    )
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("eval/questions.yaml"),
        help="Dataset path (YAML or JSON). Defaults to eval/questions.yaml.",
    )
    parser.add_argument(
        "--mode",
        choices=("single-shot", "retry"),
        default="retry",
        help="Evaluation mode.",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=3,
        help="Number of repetitions per question.",
    )
    parser.add_argument(
        "--models",
        help=(
            "Comma-separated model list. If omitted, uses EVAL_MODELS env or a "
            "built-in default list."
        ),
    )
    parser.add_argument(
        "--preset",
        choices=("all",),
        help="Use a predefined model list.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("eval/runs"),
        help="Directory to store run folders (default: eval/runs).",
    )
    parser.add_argument(
        "--model-parallelism",
        type=int,
        help=(
            "Run multiple models in parallel using separate processes. "
            "Defaults to K8S_GRAPH_EVAL_MODEL_PARALLELISM/EVAL_MODEL_PARALLELISM "
            "or 1."
        ),
    )
    args = parser.parse_args()

    models = _resolve_models(args.models, args.preset)
    if not models:
        raise SystemExit("No models provided. Use --models or set EVAL_MODELS.")
    model_parallelism = _model_parallelism(args.model_parallelism)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = args.output_dir / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "created_at": timestamp,
        "dataset": str(args.dataset),
        "mode": args.mode,
        "runs": args.runs,
        "models": models,
        "model_parallelism": model_parallelism,
        "parallelism": os.environ.get("K8S_GRAPH_EVAL_PARALLELISM")
        or os.environ.get("EVAL_PARALLELISM"),
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )

    if model_parallelism > 1 and len(models) > 1:
        _run_models_in_parallel(
            models=models,
            run_dir=run_dir,
            dataset=args.dataset,
            mode=args.mode,
            runs=args.runs,
            model_parallelism=model_parallelism,
        )
        return

    for model in models:
        provider = _provider_override(model, _detect_provider(model))
        output_path = run_dir / f"results_{_sanitize_model(model)}.jsonl"
        env_updates, env_clears = _model_env(provider, model)
        if not os.environ.get("K8S_GRAPH_LOG_FILE") and not os.environ.get(
            "K8S_GRAPH_LOG_DIR"
        ):
            env_updates["K8S_GRAPH_LOG_FILE"] = str(
                run_dir / f"eval_{_sanitize_model(model)}.log"
            )
        print(
            f"[eval] model={model} provider={provider or 'auto'} output={output_path}"
        )
        with _temp_env(env_updates, env_clears):
            run_evaluation(
                dataset_path=args.dataset,
                mode=args.mode,
                runs=args.runs,
                output_path=output_path,
            )


def _resolve_models(cli_models: str | None, preset: str | None) -> list[str]:
    if preset == "all":
        return DEFAULT_MODELS[:]
    if cli_models:
        return [m.strip() for m in cli_models.split(",") if m.strip()]
    env_models = os.environ.get("EVAL_MODELS")
    if env_models:
        return [m.strip() for m in env_models.split(",") if m.strip()]
    return DEFAULT_MODELS[:]


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


def _normalize_provider(value: str | None) -> str | None:
    if value is None:
        return None
    lowered = value.strip().lower()
    if lowered in {"gemini", "google"}:
        return "gemini"
    if lowered in {"claude"}:
        return "anthropic"
    return lowered


def _force_openai_proxy() -> bool:
    return os.environ.get("K8S_GRAPH_EVAL_FORCE_OPENAI_PROXY", "").lower() in {
        "1",
        "true",
        "yes",
    } or os.environ.get("EVAL_FORCE_OPENAI_PROXY", "").lower() in {"1", "true", "yes"}


def _provider_override(model: str, provider: str | None) -> str | None:
    lowered = model.strip().lower()
    if provider in {"anthropic"} or lowered.startswith("claude"):
        override = _normalize_provider(
            os.environ.get("K8S_GRAPH_EVAL_CLAUDE_PROVIDER")
            or os.environ.get("EVAL_CLAUDE_PROVIDER")
        )
        if override:
            return override
    if provider in {"deepseek"} or lowered.startswith("deepseek"):
        override = _normalize_provider(
            os.environ.get("K8S_GRAPH_EVAL_DEEPSEEK_PROVIDER")
            or os.environ.get("EVAL_DEEPSEEK_PROVIDER")
        )
        if override:
            return override
    if _force_openai_proxy() and (provider in {"anthropic", "deepseek"}):
        return "openai"
    return provider


def _model_env(provider: str | None, model: str) -> tuple[dict[str, str], list[str]]:
    updates: dict[str, str] = {"LLM_MODEL": model}
    clears: list[str] = ["LLM_BASE_URL"]
    if provider:
        updates["LLM_PROVIDER"] = provider
    if provider in {"openai", "openai-compatible"}:
        base_url = os.environ.get("OPENAI_BASE_URL") or os.environ.get("LLM_BASE_URL")
        if base_url:
            updates["LLM_BASE_URL"] = base_url
    elif provider == "gemini":
        base_url = os.environ.get("GOOGLE_GEMINI_BASE_URL")
        if base_url:
            updates["LLM_BASE_URL"] = base_url
    elif provider == "anthropic":
        base_url = os.environ.get("ANTHROPIC_BASE_URL") or os.environ.get(
            "CLAUDE_BASE_URL"
        )
        if base_url:
            updates["LLM_BASE_URL"] = base_url
    elif provider == "deepseek":
        base_url = os.environ.get("DEEPSEEK_BASE_URL")
        if base_url:
            updates["LLM_BASE_URL"] = base_url
    return updates, clears


def _model_parallelism(cli_value: int | None) -> int:
    if cli_value is not None:
        return max(1, cli_value)
    raw = os.environ.get("K8S_GRAPH_EVAL_MODEL_PARALLELISM") or os.environ.get(
        "EVAL_MODEL_PARALLELISM", "1"
    )
    try:
        value = int(raw)
    except ValueError:
        return 1
    return max(1, value)


def _run_models_in_parallel(
    *,
    models: list[str],
    run_dir: Path,
    dataset: Path,
    mode: str,
    runs: int,
    model_parallelism: int,
) -> None:
    ctx = get_context("spawn")
    pending = []
    failures: list[tuple[str, int]] = []

    for model in models:
        provider = _provider_override(model, _detect_provider(model))
        output_path = run_dir / f"results_{_sanitize_model(model)}.jsonl"
        process = ctx.Process(
            target=_run_model_process,
            kwargs={
                "model": model,
                "provider": provider,
                "dataset": dataset,
                "mode": mode,
                "runs": runs,
                "output_path": output_path,
                "run_dir": run_dir,
            },
        )
        process.start()
        pending.append((process, model))
        while len(pending) >= model_parallelism:
            _drain_finished(pending, failures)
            if len(pending) >= model_parallelism:
                time.sleep(0.05)

    while pending:
        _drain_finished(pending, failures)
        if pending:
            time.sleep(0.05)

    if failures:
        failed = ", ".join(f"{model} (exit {code})" for model, code in failures)
        raise SystemExit(f"Model evals failed: {failed}")


def _drain_finished(
    pending: list[tuple[BaseProcess, str]],
    failures: list[tuple[str, int]],
) -> None:
    for entry in list(pending):
        process, model = entry
        if process.is_alive():
            continue
        process.join()
        pending.remove(entry)
        if process.exitcode not in {0, None}:
            failures.append((model, process.exitcode or 1))


def _run_model_process(
    *,
    model: str,
    provider: str | None,
    dataset: Path,
    mode: str,
    runs: int,
    output_path: Path,
    run_dir: Path,
) -> None:
    env_updates, env_clears = _model_env(provider, model)
    if not os.environ.get("K8S_GRAPH_LOG_FILE") and not os.environ.get(
        "K8S_GRAPH_LOG_DIR"
    ):
        env_updates["K8S_GRAPH_LOG_FILE"] = str(
            run_dir / f"eval_{_sanitize_model(model)}.log"
        )
    print(
        f"[eval] model={model} provider={provider or 'auto'} output={output_path}",
        flush=True,
    )
    try:
        with _temp_env(env_updates, env_clears):
            run_evaluation(
                dataset_path=dataset,
                mode=mode,
                runs=runs,
                output_path=output_path,
            )
    except Exception as exc:  # pragma: no cover - subprocess crash
        print(f"[eval] model={model} failed: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1) from exc


@contextmanager
def _temp_env(updates: dict[str, str], clears: list[str]) -> Iterator[None]:
    keys = set(updates) | set(clears)
    previous = {key: os.environ.get(key) for key in keys}
    try:
        for key in clears:
            os.environ.pop(key, None)
        for key, value in updates.items():
            os.environ[key] = value
        yield
    finally:
        for key in keys:
            old_value = previous.get(key)
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _sanitize_model(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", model)


if __name__ == "__main__":
    main()
