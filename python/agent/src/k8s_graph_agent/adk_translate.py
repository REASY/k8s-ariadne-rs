from __future__ import annotations

from dataclasses import dataclass
import asyncio
import logging
import os

from pydantic import BaseModel, Field, ValidationError

from .config import AdkConfig
from .cypher_validator import CypherSchemaValidator, CypherValidationError
from .graph_schema import GraphSchema
from .mcp_client import McpClient
from .models import CypherQuery
from .prompting import extract_prompt_text


class CypherTranslation(BaseModel):
    cypher: str = Field(..., description="Cypher query to run against the graph")
    notes: str | None = Field(
        default=None,
        description="Optional clarifications or assumptions about the query",
    )
    confidence: float | None = Field(
        default=None,
        description="Optional confidence score from 0.0 to 1.0",
    )


@dataclass
class AdkCypherTranslator:
    mcp: McpClient
    config: AdkConfig

    def __post_init__(self) -> None:
        self._runner = None
        self._validator = None
        self._logger = logging.getLogger(__name__)

    def translate(self, question: str) -> CypherQuery:
        self._logger.info(
            "translate question (use_mcp_prompt=%s)", self.config.use_mcp_prompt
        )
        prompt_text = question
        if self.config.use_mcp_prompt:
            prompt = self.mcp.get_prompt("analyze_question", {"question": question})
            extracted = extract_prompt_text(prompt)
            if extracted:
                prompt_text = extracted
                self._logger.debug("using MCP prompt template")
        runner, types = self._get_runner()
        validator = self._validator
        if validator is None:
            schema = GraphSchema.load_from_mcp(self.mcp)
            if schema is None:
                self._logger.info("schema loaded from local/default config")
                schema = GraphSchema.load_default()
            else:
                self._logger.info("schema loaded from MCP")
            validator = CypherSchemaValidator(schema)
            self._validator = validator

        max_attempts = 2
        current_prompt = prompt_text
        total_usage = TokenUsage()
        last_error: Exception | None = None
        for attempt in range(1, max_attempts + 1):
            self._logger.info("cypher translation attempt %d/%d", attempt, max_attempts)
            content = types.Content(
                role="user", parts=[types.Part(text=current_prompt)]
            )
            response_text, usage = _run_agent(runner, self.config, content)
            total_usage.add(usage)
            try:
                translation = CypherTranslation.model_validate_json(response_text)
            except ValidationError as exc:
                last_error = ValueError(f"ADK output did not match schema: {exc}")
                break
            cypher = translation.cypher.strip()
            if not cypher:
                last_error = ValueError("ADK returned empty Cypher query")
                break
            self._logger.debug("cypher candidate:\n%s", cypher)
            try:
                validator.validate(cypher)
            except CypherValidationError as exc:
                self._logger.warning("cypher validation failed: %s", exc)
                self._logger.warning("invalid cypher:\n%s", cypher)
                if attempt < max_attempts:
                    current_prompt = _build_retry_prompt(prompt_text, cypher, str(exc))
                    continue
                last_error = ValueError(str(exc))
                break
            self._logger.info("cypher validation succeeded")
            _log_total_usage(self._logger, total_usage)
            return CypherQuery(text=cypher)

        _log_total_usage(self._logger, total_usage)
        if last_error is not None:
            raise last_error
        raise ValueError("Cypher translation failed after retries")

    def _get_runner(self) -> tuple[object, object]:
        if self._runner is not None:
            return self._runner
        try:
            from google.adk.agents import Agent
            from google.adk.models.lite_llm import LiteLlm
            from google.adk.runners import Runner
            from google.adk.sessions import InMemorySessionService
            from google.genai import types
            import litellm
        except ImportError as exc:  # pragma: no cover - exercised in integration only
            raise ImportError(
                "google-adk or litellm is not installed. "
                "Install with `uv pip install -e .` or `uv sync`"
            ) from exc

        model_name = _format_model(self.config.model, self.config.provider)
        if self.config.provider in {"google", "gemini"} and self.config.api_key:
            os.environ.setdefault("GOOGLE_API_KEY", self.config.api_key)

        lite_llm_kwargs: dict[str, object] = {}
        if self.config.api_key:
            lite_llm_kwargs["api_key"] = self.config.api_key
        if self.config.base_url:
            lite_llm_kwargs["api_base"] = self.config.base_url

        litellm.set_verbose = False

        instruction = (
            "You translate questions about a Kubernetes graph into a single Cypher query. "
            "Always respect the schema and query rules included in the prompt. "
            "Return only JSON with keys: cypher (string), optional notes (string), "
            "optional confidence (number between 0 and 1)."
        )
        agent = Agent(
            name="cypher_translator",
            model=LiteLlm(model=model_name, **lite_llm_kwargs),
            instruction=instruction,
            generate_content_config=types.GenerateContentConfig(
                temperature=self.config.temperature,
                max_output_tokens=self.config.max_output_tokens,
            ),
        )
        session_service = InMemorySessionService()
        _run_async(
            session_service.create_session(
                app_name=self.config.app_name,
                user_id=self.config.user_id,
                session_id=self.config.session_id,
            )
        )
        self._runner = (
            Runner(
                agent=agent,
                app_name=self.config.app_name,
                session_service=session_service,
            ),
            types,
        )
        return self._runner


def _format_model(model: str, provider: str | None) -> str:
    normalized = model.strip()
    if "/" in normalized:
        return normalized
    if provider:
        return f"{provider}/{normalized}"
    return normalized


def _run_agent(
    runner: object, config: AdkConfig, content: object
) -> tuple[str, "TokenUsage"]:
    response_text = ""
    usage = TokenUsage()
    for event in runner.run(
        user_id=config.user_id,
        session_id=config.session_id,
        new_message=content,
    ):
        usage.update_from_event(event)
        if getattr(event, "is_final_response")() and getattr(event, "content", None):
            parts = getattr(event.content, "parts", [])
            if parts:
                text = getattr(parts[0], "text", None)
                if isinstance(text, str):
                    response_text = text
    if not response_text:
        raise ValueError("ADK returned no response content")
    usage.log_if_present()
    return response_text, usage


def _build_retry_prompt(base_prompt: str, cypher: str, error: str) -> str:
    return (
        f"{base_prompt}\n\n"
        "The previous Cypher failed schema validation.\n"
        f"Error: {error}\n"
        "Previous Cypher:\n"
        f"{cypher}\n"
        "Fix the query to satisfy the schema and rules. "
        "Return only JSON with keys: cypher, optional notes, optional confidence."
    )


def _run_async(coro: object) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
        return
    raise RuntimeError(
        "ADK session setup requires a sync context. "
        "Call AdkCypherTranslator from a non-async entrypoint."
    )


class TokenUsage:
    def __init__(self) -> None:
        self.prompt_tokens: int | None = None
        self.output_tokens: int | None = None
        self.total_tokens: int | None = None
        self._logger = logging.getLogger(__name__)

    def update_from_event(self, event: object) -> None:
        usage = getattr(event, "usage_metadata", None)
        if usage is None:
            return
        prompt = _read_usage_value(
            usage, ["prompt_token_count", "prompt_tokens", "input_tokens"]
        )
        output = _read_usage_value(
            usage,
            ["candidates_token_count", "completion_tokens", "output_tokens"],
        )
        total = _read_usage_value(usage, ["total_token_count", "total_tokens"])
        self.prompt_tokens = _coalesce_usage(self.prompt_tokens, prompt)
        self.output_tokens = _coalesce_usage(self.output_tokens, output)
        self.total_tokens = _coalesce_usage(self.total_tokens, total)

    def log_if_present(self) -> None:
        if (
            self.prompt_tokens is None
            and self.output_tokens is None
            and self.total_tokens is None
        ):
            return
        self._logger.info(
            "adk tokens: prompt=%s output=%s total=%s",
            _format_usage(self.prompt_tokens),
            _format_usage(self.output_tokens),
            _format_usage(self.total_tokens),
        )

    def add(self, other: "TokenUsage") -> None:
        self.prompt_tokens = _sum_usage(self.prompt_tokens, other.prompt_tokens)
        self.output_tokens = _sum_usage(self.output_tokens, other.output_tokens)
        self.total_tokens = _sum_usage(self.total_tokens, other.total_tokens)

    def has_any(self) -> bool:
        return (
            self.prompt_tokens is not None
            or self.output_tokens is not None
            or self.total_tokens is not None
        )


def _read_usage_value(usage: object, keys: list[str]) -> int | None:
    for key in keys:
        if isinstance(usage, dict) and key in usage:
            value = usage.get(key)
            if isinstance(value, int):
                return value
        value = getattr(usage, key, None)
        if isinstance(value, int):
            return value
    return None


def _coalesce_usage(existing: int | None, incoming: int | None) -> int | None:
    if incoming is None:
        return existing
    if existing is None:
        return incoming
    return max(existing, incoming)


def _sum_usage(existing: int | None, incoming: int | None) -> int | None:
    if incoming is None:
        return existing
    if existing is None:
        return incoming
    return existing + incoming


def _log_total_usage(logger: logging.Logger, usage: TokenUsage) -> None:
    if not usage.has_any():
        return
    logger.info(
        "adk tokens (all attempts): prompt=%s output=%s total=%s",
        _format_usage(usage.prompt_tokens),
        _format_usage(usage.output_tokens),
        _format_usage(usage.total_tokens),
    )


def _format_usage(value: int | None) -> str:
    return str(value) if value is not None else "-"
