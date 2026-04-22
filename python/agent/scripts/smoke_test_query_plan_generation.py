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
    _is_openai_provider,
    _run_async,
    _strip_code_fences,
    _strip_provider_prefix,
    _summarize_event,
)
from k8s_graph_agent.config import AdkConfig, AgentConfig
from k8s_graph_agent.graph_schema import GraphSchema
from k8s_graph_agent.mcp_client import StreamableHttpMcpClient, extract_json_content
from k8s_graph_agent.prompting import prompt_sections_from_graph_schema_payload
from k8s_graph_agent.query_plan import TranslatorOutput
from k8s_graph_agent.query_plan_validator import QueryPlanValidationError, validate_translator_output


@dataclass(frozen=True)
class Example:
    id: str
    question: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke-test LLM generation of QueryPlan V1 TranslatorOutput JSON."
    )
    parser.add_argument(
        "--examples",
        type=Path,
        default=Path("eval/query_plan_v1_examples.yaml"),
        help="Handwritten example set to reuse for question selection.",
    )
    parser.add_argument(
        "--models",
        default="openai/gpt-5-mini-2025-08-07,gemini-2.5-flash",
        help="Comma-separated models to test.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Max number of questions to smoke-test.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("eval/query_plan_smoke"),
        help="Directory for smoke-test artifacts.",
    )
    parser.add_argument(
        "--prompt-fallback",
        type=Path,
        default=Path("adk_config/k8s_graph_agent/root_agent.yaml"),
        help="Prompt file to use if MCP is unavailable.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    examples = _load_examples(args.examples)[: args.limit]
    run_dir = args.output_dir / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    agent_config = AgentConfig.from_env()
    mcp = StreamableHttpMcpClient(
        base_url=agent_config.mcp_url,
        timeout_seconds=agent_config.request_timeout_seconds,
        client_name=agent_config.client_name,
        client_version=agent_config.client_version,
        auth_token=agent_config.mcp_auth_token,
    )
    schema = GraphSchema.load_default()

    try:
        try:
            schema_payload = extract_json_content(
                mcp.call_tool("graph_schema", {"format": "structured"})
            )
            prompt_sections = (
                prompt_sections_from_graph_schema_payload(schema_payload)
                if isinstance(schema_payload, dict)
                else None
            )
        except Exception:
            prompt_sections = None
        if prompt_sections is None:
            fallback_path = args.prompt_fallback
            if not fallback_path.is_absolute():
                fallback_path = Path.cwd() / fallback_path
            from k8s_graph_agent.prompting import split_prompt_sections

            prompt_text = fallback_path.read_text(encoding="utf-8")
            sections = split_prompt_sections(prompt_text)
        else:
            sections = prompt_sections

        manifest = {
            "examples": [example.id for example in examples],
            "models": [model.strip() for model in args.models.split(",") if model.strip()],
            "schema_reference_chars": len(sections.schema_reference),
            "node_connectivity_chars": len(sections.node_connectivity),
        }
        (run_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
        )

        for model in [model.strip() for model in args.models.split(",") if model.strip()]:
            report = _run_model_smoke(model, examples, sections.schema_reference, sections.node_connectivity, schema)
            model_slug = _slugify(model)
            model_dir = run_dir / model_slug
            model_dir.mkdir(parents=True, exist_ok=True)
            (model_dir / "report.json").write_text(
                json.dumps(report, indent=2, sort_keys=True), encoding="utf-8"
            )
            print(
                f"[smoke] {model}: parsed={report['summary']['parsed_ok']}/{report['summary']['total']} "
                f"valid={report['summary']['semantically_valid']}/{report['summary']['total']}"
            )
    finally:
        mcp.close()
    return 0


def _load_examples(path: Path) -> list[Example]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a top-level list")
    examples: list[Example] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        if "question" in item and "id" in item:
            examples.append(Example(id=str(item["id"]), question=str(item["question"])))
    return examples


def _run_model_smoke(
    model: str,
    examples: list[Example],
    schema_reference: str,
    node_connectivity: str,
    schema: GraphSchema,
) -> dict[str, Any]:
    config = _config_for_model(model)
    runner, types, session_service = _build_runner_for_model(config)
    results: dict[str, Any] = {}
    parsed_ok = 0
    semantically_valid = 0
    plan_mode = 0
    cypher_mode = 0

    for example in examples:
        prompt = _build_query_plan_prompt(example.question, schema_reference, node_connectivity)
        session_id = f"{config.session_id}-{uuid.uuid4().hex}"
        _run_async(
            session_service.create_session(
                app_name=config.app_name,
                user_id=config.user_id,
                session_id=session_id,
            )
        )
        content = types.Content(role="user", parts=[types.Part(text=prompt)])
        try:
            response_text, usage, debug = _run_agent_debug(
                runner, config, content, session_id
            )
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
                "tokens": {
                    "prompt": usage.prompt_tokens,
                    "output": usage.output_tokens,
                    "total": usage.total_tokens,
                },
            }
            continue

        if parsed.mode == "plan":
            plan_mode += 1
        else:
            cypher_mode += 1

        try:
            validate_translator_output(parsed, schema=schema)
            semantically_valid += 1
            valid = True
            validation_error = None
        except QueryPlanValidationError as exc:
            valid = False
            validation_error = [issue.__dict__ for issue in exc.issues]

        results[example.id] = {
            "question": example.question,
            "parsed": True,
            "valid": valid,
            "mode": parsed.mode,
            "validation_error": validation_error,
            "debug": debug,
            "response": parsed.model_dump(mode="json", by_alias=True),
            "tokens": {
                "prompt": usage.prompt_tokens,
                "output": usage.output_tokens,
                "total": usage.total_tokens,
            },
        }

    return {
        "model": model,
        "summary": {
            "total": len(examples),
            "parsed_ok": parsed_ok,
            "semantically_valid": semantically_valid,
            "plan_mode": plan_mode,
            "cypher_mode": cypher_mode,
        },
        "results": results,
    }


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
                    raw_parts.append(
                        {
                            "thought": bool(thought),
                            "text": text,
                        }
                    )
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


def _build_query_plan_prompt(
    question: str, schema_reference: str, node_connectivity: str
) -> str:
    return f"""You translate Kubernetes graph questions into structured JSON.

Return only JSON matching this root schema:
- mode: "plan" or "cypher"
- if mode="plan": include plan with $schema="QueryPlanV1"
- if mode="cypher": include read-only cypher plus a short reason

Use mode="plan" whenever the question can be represented by QueryPlanV1.
Only use mode="cypher" for features outside QueryPlanV1 scope such as UNION, regex, CALL procedures, shortest paths, FOREACH, REDUCE/list comprehensions, variable-length paths, or path bindings.

QueryPlanV1 concepts:
- match: ordered MatchStep list over typed entities
- MatchStep.from: relationship to a previous variable, direction inferred from schema
- filter: property predicates, property-vs-property, alias predicates, boolean composition
- optional: OPTIONAL MATCH semantics
- not_exists: NegationClause using nested match steps
- property_join: equality join for cases with no graph relationship
- where: post-match global filters
- unwind: one UNWIND for nested array properties
- stages: aggregation pipeline with group_by, compute, having
- return: entity property, stage_ref, or coalesce
- order_by, limit, distinct

Key semantic rules:
- Use typed entity and relationship names from the schema/connectivity below.
- Prefer plan mode for ordinary list/filter/traversal/aggregation questions.
- The output must be a single JSON object and nothing else.
- If you choose cypher mode, the cypher must be read-only.

Schema reference:
{schema_reference}

Node connectivity:
{node_connectivity}

Question: {question}
"""


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
        api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or api_key
    elif provider == "anthropic":
        base_url = os.environ.get("CLAUDE_BASE_URL") or os.environ.get("ANTHROPIC_BASE_URL") or base_url
        api_key = os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_API_KEY") or api_key
    temperature = base.temperature
    lowered = model.strip().lower()
    if lowered.startswith("openai/gpt-5") or lowered.startswith("gpt-5"):
        temperature = 1.0
    return AdkConfig(
        model=model,
        provider=provider,
        base_url=base_url,
        api_key=api_key,
        app_name=base.app_name,
        user_id=base.user_id,
        session_id=f"query-plan-smoke-{uuid.uuid4().hex[:8]}",
        temperature=temperature,
        max_output_tokens=min(base.max_output_tokens, 8192),
        use_mcp_prompt=False,
    )


def _infer_provider_override(model: str, default_provider: str | None) -> str | None:
    lowered = model.lower()
    if lowered.startswith("openai/"):
        return "openai"
    if lowered.startswith("gemini") or lowered.startswith("google/") or lowered.startswith("google/gemini"):
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
        os.environ.setdefault("GOOGLE_API_KEY", config.api_key)
    if use_native_anthropic and config.api_key:
        os.environ.setdefault("ANTHROPIC_API_KEY", config.api_key)
    if use_native_anthropic and config.base_url:
        os.environ.setdefault("ANTHROPIC_BASE_URL", config.base_url)

    lite_llm_kwargs: dict[str, Any] = {}
    if config.api_key and not use_native_gemini:
        lite_llm_kwargs["api_key"] = config.api_key
    if config.base_url and not use_native_gemini:
        lite_llm_kwargs["api_base"] = config.base_url

    litellm.set_verbose = False
    instruction = (
        "You produce TranslatorOutput JSON for Kubernetes graph questions. "
        "Return only JSON. Prefer mode='plan' whenever QueryPlanV1 can express the question. "
        "Use mode='cypher' only for truly unsupported read-only features."
    )
    generate_config = _build_generate_content_config(config, types)
    if use_native_gemini:
        model = Gemini(model=model_name)
    elif use_native_anthropic:
        from google.adk.models.anthropic_llm import AnthropicLlm

        model = AnthropicLlm(model=model_name, max_tokens=config.max_output_tokens)
    else:
        model = LiteLlm(model=model_name, **lite_llm_kwargs)
    agent = Agent(
        name="query_plan_smoke",
        model=model,
        instruction=instruction,
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


def _slugify(model: str) -> str:
    return model.replace("/", "_").replace(".", "_")


if __name__ == "__main__":
    raise SystemExit(main())
