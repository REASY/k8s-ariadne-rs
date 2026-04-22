from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime
import json
import os
from pathlib import Path
import uuid
from typing import Any

import yaml

from k8s_graph_agent.adk_translate import (
    TokenUsage,
    _build_generate_content_config,
    _format_model,
    _is_anthropic_provider,
    _is_gemini_provider,
    _run_async,
    _strip_code_fences,
    _strip_provider_prefix,
    _summarize_event,
)
from k8s_graph_agent.config import AdkConfig
from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.query_plan import TranslatorOutput
from k8s_graph_agent.query_plan_validator import QueryPlanValidationError, validate_translator_output


@dataclass(frozen=True)
class Example:
    id: str
    question: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test full QueryPlanV1 on hard constructs."
    )
    parser.add_argument(
        "--examples",
        type=Path,
        default=Path("eval/query_plan_v1_hard_examples.yaml"),
    )
    parser.add_argument(
        "--models",
        default="openai/gpt-5-mini-2025-08-07,gemini-2.5-flash",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("eval/query_plan_hard_smoke"),
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    examples = _load_examples(args.examples)[: args.limit]
    run_dir = args.output_dir / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)
    schema = GraphSchema.load_default()

    manifest = {
        "examples": [example.id for example in examples],
        "models": [model.strip() for model in args.models.split(",") if model.strip()],
        "schema": "QueryPlanV1",
    }
    (run_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )

    for model in [model.strip() for model in args.models.split(",") if model.strip()]:
        report = _run_model_smoke(model, examples, schema)
        model_slug = _slugify(model)
        model_dir = run_dir / model_slug
        model_dir.mkdir(parents=True, exist_ok=True)
        (model_dir / "report.json").write_text(
            json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
        )
        print(
            f"[hard-smoke] {model}: parsed={report['summary']['parsed_ok']}/{report['summary']['total']} "
            f"valid={report['summary']['semantically_valid']}/{report['summary']['total']}"
        )
    return 0


def _load_examples(path: Path) -> list[Example]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a top-level list")
    return [Example(id=str(item["id"]), question=str(item["question"])) for item in payload]


def _run_model_smoke(model: str, examples: list[Example], schema: GraphSchema) -> dict[str, Any]:
    config = _config_for_model(model)
    runner, types, session_service = _build_runner_for_model(config)
    parsed_ok = 0
    semantically_valid = 0
    results: dict[str, Any] = {}

    for example in examples:
        session_id = f"{config.session_id}-{uuid.uuid4().hex}"
        _run_async(
            session_service.create_session(
                app_name=config.app_name,
                user_id=config.user_id,
                session_id=session_id,
            )
        )
        prompt = _build_hard_prompt(example.question)
        content = types.Content(role="user", parts=[types.Part(text=prompt)])
        try:
            response_text, usage, debug = _run_agent_debug(runner, config, content, session_id)
        except Exception as exc:
            results[example.id] = {
                "question": example.question,
                "parsed": False,
                "valid": False,
                "error": str(exc),
            }
            continue

        try:
            parsed = TranslatorOutput.model_validate_json(_strip_code_fences(response_text))
            parsed_ok += 1
        except Exception as exc:
            results[example.id] = {
                "question": example.question,
                "parsed": False,
                "valid": False,
                "error": f"parse_error: {exc}",
                "raw_response": response_text,
                "debug": debug,
                "tokens": _usage_dict(usage),
            }
            continue

        try:
            validate_translator_output(parsed, schema=schema)
            semantically_valid += 1
            validation_error = None
            valid = True
        except QueryPlanValidationError as exc:
            valid = False
            validation_error = [issue.__dict__ for issue in exc.issues]

        results[example.id] = {
            "question": example.question,
            "parsed": True,
            "valid": valid,
            "validation_error": validation_error,
            "response": parsed.model_dump(mode="json", by_alias=True),
            "debug": debug,
            "tokens": _usage_dict(usage),
        }

    return {
        "model": model,
        "summary": {
            "total": len(examples),
            "parsed_ok": parsed_ok,
            "semantically_valid": semantically_valid,
        },
        "results": results,
    }


def _build_hard_prompt(question: str) -> str:
    return _build_concise_ir_prompt(question)


def _build_concise_ir_prompt(question: str, schema_context: str = "") -> str:
    """Build a concise IR prompt. If schema_context is provided, it is
    prepended (schema-first mode). Otherwise the prompt is standalone."""
    ir_contract = _IR_CONTRACT_SECTION
    example = _FEWSHOT_EXAMPLE
    parts = []
    if schema_context:
        parts.append(schema_context)
        parts.append("")
    parts.append("## QueryPlanV1 output format")
    parts.append("")
    parts.append(_OUTPUT_SKELETON)
    parts.append("")
    parts.append(ir_contract)
    parts.append("")
    parts.append("Example:")
    parts.append(example)
    parts.append("")
    parts.append(f"Q: {question}")
    return "\n".join(parts)


_OUTPUT_SKELETON = """\
Return only JSON matching this structure:
{
  "mode": "plan",
  "plan": {
    "$schema": "QueryPlanV1",
    "match": [ MatchStep, ... ],
    "where": [],
    "unwind": null,
    "stages": [],
    "return": [ ReturnExpr, ... ],
    "order_by": [],
    "limit": null,
    "distinct": false
  }
}
All fields except "match" and "return" may be omitted when empty/null."""

_IR_CONTRACT_SECTION = """\
MatchStep: { "entity": EntityType, "bind": name|null, "from": {"variable": v, "relationship": r}|null, "filter": [...], "optional": bool }
Filter: { "property": name, "op": "eq"|"neq"|"gt"|"lt"|"gte"|"lte"|"is_null"|"is_not_null", "value": literal }
Return: { "variable": v, "property": p, "alias": a } or { "stage_ref": alias } or { "coalesce": [...], "alias": a }
Stage: { "group_by": [{"variable":v} or {"variable":v,"property":p,"alias":a}], "compute": [{"fn":f,"input":v,"alias":a}], "having": [{"alias":a,"op":op,"value":val} or {"variable":v,"op":"is_not_null"}] }
Compute fns: count, count_distinct, collect_distinct, sum, sum_memory_mib, size
For collect_distinct over a property: { "fn": "collect_distinct", "input": v, "input_property": p, "alias": a }
For size of a prior alias: { "fn": "size", "input": prior_alias, "alias": a }

Key rules:
- Compiler infers relationship direction from the schema. Do not specify direction.
- Deployment → Pod goes through ReplicaSet: Deployment -[:Manages]-> ReplicaSet -[:Manages]-> Pod
- Service backing chain: Service -[:Manages]-> EndpointSlice -[:ContainsEndpoint]-> Endpoint -[:HasAddress]-> EndpointAddress -[:IsAddressOf]-> Pod
- EndpointAddress uses property "address", not "ip".
- Use unwind only for spec.containers with element_type "k8s_container_spec".
- Omit "bind" for anonymous intermediate hops not referenced later.
- Empty arrays may be omitted (filter, where, stages, order_by)."""

_FEWSHOT_EXAMPLE = """\
Q: List all pods in namespace litmus.
{"mode":"plan","plan":{"$schema":"QueryPlanV1","match":[{"entity":"Namespace","bind":"ns","filter":[{"property":"name","op":"eq","value":"litmus"}]},{"entity":"Pod","bind":"p","from":{"variable":"ns","relationship":"BelongsTo"}}],"return":[{"variable":"p","property":"name","alias":"pod"}],"order_by":[{"column":"pod","direction":"asc"}]}}"""


def _config_for_model(model: str) -> AdkConfig:
    base = AdkConfig.from_env()
    provider = _infer_provider_override(model, base.provider)
    base_url = base.base_url
    api_key = base.api_key
    if provider in {"openai", "openai-compatible"}:
        base_url = os.environ.get("OPENAI_BASE_URL", base_url)
        api_key = os.environ.get("OPENAI_API_KEY", api_key)
    elif provider == "gemini":
        base_url = os.environ.get("GOOGLE_GEMINI_BASE_URL", base_url)
        api_key = os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY") or api_key
    elif provider == "anthropic":
        base_url = os.environ.get("CLAUDE_BASE_URL") or os.environ.get("ANTHROPIC_BASE_URL") or base_url
        api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY") or api_key
    temperature = 1.0 if model.strip().lower().startswith(("openai/gpt-5", "gpt-5")) else base.temperature
    return AdkConfig(
        model=model,
        provider=provider,
        base_url=base_url,
        api_key=api_key,
        app_name=base.app_name,
        user_id=base.user_id,
        session_id=f"query-plan-hard-{uuid.uuid4().hex[:8]}",
        temperature=temperature,
        max_output_tokens=min(base.max_output_tokens, 4096),
        use_mcp_prompt=False,
    )


def _infer_provider_override(model: str, default_provider: str | None) -> str | None:
    lowered = model.lower()
    if lowered.startswith("openai/"):
        return "openai"
    if lowered.startswith("gemini") or lowered.startswith("google/gemini"):
        return "gemini"
    if lowered.startswith("claude") or lowered.startswith("anthropic/"):
        return "anthropic"
    return default_provider


def _build_runner_for_model(config: AdkConfig) -> tuple[Any, Any, Any]:
    try:
        from google.adk.agents import Agent
        from google.adk.models import Gemini
        from google.adk.models.lite_llm import LiteLlm
        from google.adk.runners import Runner
        from google.adk.sessions import InMemorySessionService
        from google.genai import types
        import litellm
    except ImportError as exc:  # pragma: no cover
        raise ImportError("google-adk/litellm not installed") from exc

    use_native_gemini = _is_gemini_provider(config.provider, config.model)
    use_native_anthropic = _is_anthropic_provider(config.provider, config.model)
    if use_native_gemini or use_native_anthropic:
        model_name = _strip_provider_prefix(config.model)
    else:
        model_name = _format_model(config.model, config.provider)

    if use_native_gemini and config.api_key:
        os.environ["GOOGLE_API_KEY"] = config.api_key
    if use_native_anthropic and config.api_key:
        os.environ["ANTHROPIC_API_KEY"] = config.api_key
    if use_native_anthropic and config.base_url:
        os.environ["ANTHROPIC_BASE_URL"] = config.base_url

    lite_llm_kwargs: dict[str, Any] = {}
    if config.api_key and not use_native_gemini:
        lite_llm_kwargs["api_key"] = config.api_key
    if config.base_url and not use_native_gemini:
        lite_llm_kwargs["api_base"] = config.base_url

    litellm.set_verbose = False
    generate_config = _build_generate_content_config(config, types)
    generate_config.response_mime_type = "application/json"
    if use_native_gemini:
        model = Gemini(model=model_name)
    elif use_native_anthropic:
        from google.adk.models.anthropic_llm import AnthropicLlm

        model = AnthropicLlm(model=model_name, max_tokens=config.max_output_tokens)
    else:
        model = LiteLlm(model=model_name, **lite_llm_kwargs)
    agent = Agent(
        name="query_plan_hard_smoke",
        model=model,
        instruction="Return only JSON matching TranslatorOutput in plan mode.",
        generate_content_config=generate_config,
        output_schema=None,
    )
    session_service = InMemorySessionService()
    runner = Runner(
        agent=agent,
        app_name=config.app_name,
        session_service=session_service,
    )
    return runner, types, session_service


def _run_agent_debug(
    runner: Any, config: AdkConfig, content: Any, session_id: str
) -> tuple[str, TokenUsage, dict[str, Any]]:
    response_text = ""
    usage = TokenUsage()
    last_event_summary: str | None = None
    event_summaries: list[str] = []
    raw_parts: list[dict[str, Any]] = []

    for event in runner.run(
        user_id=config.user_id,
        session_id=session_id,
        new_message=content,
    ):
        usage.update_from_event(event)
        last_event_summary = _summarize_event(event)
        if len(event_summaries) < 20:
            event_summaries.append(last_event_summary)
        content_obj = getattr(event, "content", None)
        parts = getattr(content_obj, "parts", None)
        if isinstance(parts, list):
            for part in parts[:10]:
                text = getattr(part, "text", None)
                thought = getattr(part, "thought", False)
                if text is not None and len(raw_parts) < 20:
                    raw_parts.append({"thought": bool(thought), "text": text})
                if isinstance(text, str) and not thought:
                    response_text += text

    debug = {
        "last_event": last_event_summary,
        "event_summaries": event_summaries,
        "raw_parts": raw_parts,
    }
    if not response_text:
        raise ValueError(
            f"ADK returned no response content; debug={json.dumps(debug, ensure_ascii=True)}"
        )
    return response_text, usage, debug


def _usage_dict(usage: TokenUsage) -> dict[str, int | None]:
    return {
        "prompt": usage.prompt_tokens,
        "output": usage.output_tokens,
        "total": usage.total_tokens,
    }


def _slugify(model: str) -> str:
    return model.replace("/", "_").replace(".", "_")


if __name__ == "__main__":
    raise SystemExit(main())
