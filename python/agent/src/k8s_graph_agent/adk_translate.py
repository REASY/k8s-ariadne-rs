from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
import logging
import os
import re
import uuid
from typing import Any, Coroutine

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


@dataclass(frozen=True)
class TranslationAttempt:
    attempt: int
    cypher: str | None
    valid: bool
    error: str | None
    usage: "TokenUsage"


@dataclass(frozen=True)
class TranslationOutcome:
    cypher: str | None
    attempts: list[TranslationAttempt]
    total_usage: "TokenUsage"
    error: str | None


@dataclass
class AdkCypherTranslator:
    mcp: McpClient
    config: AdkConfig
    _runner: tuple[Any, Any] | None = field(init=False, default=None)
    _validator: CypherSchemaValidator | None = field(init=False, default=None)
    _session_service: Any | None = field(init=False, default=None)
    _logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def translate(self, question: str) -> CypherQuery:
        outcome = self.translate_with_attempts(question, max_attempts=2)
        if outcome.cypher is None:
            raise ValueError(outcome.error or "Cypher translation failed after retries")
        return CypherQuery(text=outcome.cypher)

    def translate_with_attempts(
        self, question: str, max_attempts: int = 2
    ) -> TranslationOutcome:
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

        current_prompt = prompt_text
        total_usage = TokenUsage()
        attempts: list[TranslationAttempt] = []
        last_error: str | None = None
        base_session_id = f"{self.config.session_id}-{uuid.uuid4().hex}"
        for attempt in range(1, max_attempts + 1):
            self._logger.info("cypher translation attempt %d/%d", attempt, max_attempts)
            session_id = f"{base_session_id}-a{attempt}"
            self._ensure_session(session_id)
            content = types.Content(
                role="user", parts=[types.Part(text=current_prompt)]
            )
            try:
                response_text, usage = _run_agent(
                    runner, self.config, content, session_id
                )
                total_usage.add(usage)
            except Exception as exc:
                error = str(exc)
                last_error = error
                self._logger.warning("cypher generation failed: %s", error)
                attempts.append(
                    TranslationAttempt(
                        attempt=attempt,
                        cypher=None,
                        valid=False,
                        error=error,
                        usage=TokenUsage(),
                    )
                )
                if attempt < max_attempts:
                    if (
                        _is_context_length_error(exc)
                        and self.config.use_mcp_prompt
                        and current_prompt != question
                    ):
                        self._logger.warning(
                            "context length exceeded; retrying with raw question"
                        )
                        current_prompt = question
                        continue
                break
            cypher: str | None = None
            try:
                cleaned_response = _strip_code_fences(response_text)
                if cleaned_response != response_text:
                    self._logger.debug("stripped code fences from LLM response")
                translation = CypherTranslation.model_validate_json(cleaned_response)
                cypher = translation.cypher.strip()
                if not cypher:
                    raise ValueError("ADK returned empty Cypher query")
                self._logger.debug("cypher candidate:\n%s", cypher)
                validator.validate(cypher)
                self._logger.info("cypher validation succeeded")
                attempts.append(
                    TranslationAttempt(
                        attempt=attempt,
                        cypher=cypher,
                        valid=True,
                        error=None,
                        usage=usage,
                    )
                )
                _log_total_usage(self._logger, total_usage)
                return TranslationOutcome(
                    cypher=cypher,
                    attempts=attempts,
                    total_usage=total_usage,
                    error=None,
                )
            except (ValidationError, CypherValidationError, ValueError) as exc:
                error = (
                    f"ADK output did not match schema: {exc}"
                    if isinstance(exc, ValidationError)
                    else str(exc)
                )
                last_error = error
                self._logger.warning("cypher validation failed: %s", error)
                dump_path = _dump_response_text(
                    response_text=response_text,
                    reason="invalid_json",
                    attempt=attempt,
                    session_id=session_id,
                )
                if dump_path:
                    self._logger.warning("raw LLM response dumped to %s", dump_path)
                if cypher:
                    self._logger.warning("invalid cypher:\n%s", cypher)
                attempts.append(
                    TranslationAttempt(
                        attempt=attempt,
                        cypher=cypher,
                        valid=False,
                        error=error,
                        usage=usage,
                    )
                )
                if attempt < max_attempts:
                    current_prompt = _build_retry_prompt(
                        prompt_text, cypher or "", error
                    )
                    continue
                break

        _log_total_usage(self._logger, total_usage)
        return TranslationOutcome(
            cypher=None,
            attempts=attempts,
            total_usage=total_usage,
            error=last_error or "Cypher translation failed after retries",
        )

    def _get_runner(self) -> tuple[Any, Any]:
        if self._runner is not None:
            return self._runner
        try:
            from google.adk.agents import Agent
            from google.adk.models import Gemini
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

        use_native_gemini = _is_gemini_provider(self.config.provider, self.config.model)
        use_native_anthropic = _is_anthropic_provider(
            self.config.provider, self.config.model
        )
        if use_native_gemini or use_native_anthropic:
            model_name = _strip_provider_prefix(self.config.model)
        else:
            model_name = _format_model(self.config.model, self.config.provider)

        if use_native_gemini and self.config.api_key:
            os.environ.setdefault("GOOGLE_API_KEY", self.config.api_key)
        if use_native_anthropic and self.config.api_key:
            os.environ.setdefault("ANTHROPIC_API_KEY", self.config.api_key)
        if use_native_anthropic and self.config.base_url:
            os.environ.setdefault("ANTHROPIC_BASE_URL", self.config.base_url)

        lite_llm_kwargs: dict[str, Any] = {}
        if self.config.api_key and not use_native_gemini:
            lite_llm_kwargs["api_key"] = self.config.api_key
        if self.config.base_url and not use_native_gemini:
            lite_llm_kwargs["api_base"] = self.config.base_url

        litellm.set_verbose = False

        instruction = (
            "You translate questions about a Kubernetes graph into a single Cypher query. "
            "Always respect the schema and query rules included in the prompt. "
            "Return only JSON with keys: cypher (string), optional notes (string), "
            "optional confidence (number between 0 and 1)."
        )
        generate_config = _build_generate_content_config(self.config, types)
        output_schema = CypherTranslation if use_native_gemini else None
        if use_native_gemini:
            model = Gemini(model=model_name)
        elif use_native_anthropic:
            try:
                from google.adk.models.anthropic_llm import AnthropicLlm
            except ImportError as exc:  # pragma: no cover - integration only
                raise ImportError(
                    "Anthropic support requires google-adk[extensions] and anthropic."
                ) from exc
            model = AnthropicLlm(
                model=model_name, max_tokens=self.config.max_output_tokens
            )
        else:
            model = LiteLlm(model=model_name, **lite_llm_kwargs)

        agent = Agent(
            name="cypher_translator",
            model=model,
            instruction=instruction,
            generate_content_config=generate_config,
            output_schema=output_schema,
        )
        session_service = InMemorySessionService()
        self._session_service = session_service
        self._runner = (
            Runner(
                agent=agent,
                app_name=self.config.app_name,
                session_service=session_service,
            ),
            types,
        )
        return self._runner

    def _ensure_session(self, session_id: str) -> None:
        if self._session_service is None:
            return
        _run_async(
            self._session_service.create_session(
                app_name=self.config.app_name,
                user_id=self.config.user_id,
                session_id=session_id,
            )
        )


def _format_model(model: str, provider: str | None) -> str:
    normalized = model.strip()
    if "/" in normalized:
        return normalized
    if provider:
        return f"{provider}/{normalized}"
    return normalized


def _strip_provider_prefix(model: str) -> str:
    normalized = model.strip()
    if "/" in normalized:
        return normalized.split("/", 1)[1]
    return normalized


def _is_gemini_provider(provider: str | None, model: str) -> bool:
    if provider and provider.strip().lower() in {"gemini", "google"}:
        return True
    return model.strip().lower().startswith(("gemini", "google/gemini", "gemini/"))


def _is_anthropic_provider(provider: str | None, model: str) -> bool:
    if provider is not None:
        return provider.strip().lower() in {"anthropic", "claude"}
    normalized = model.strip().lower()
    return normalized.startswith(
        ("anthropic/claude", "claude/")
    ) or normalized.startswith("claude")


def _build_generate_content_config(config: AdkConfig, types: Any) -> Any:
    kwargs: dict[str, Any] = {
        "temperature": config.temperature,
        "max_output_tokens": config.max_output_tokens,
    }
    http_options = _build_http_options(config, types)
    if http_options is not None:
        kwargs["http_options"] = http_options

    if _is_gemini_provider(config.provider, config.model):
        kwargs["response_mime_type"] = "application/json"

    return types.GenerateContentConfig(**kwargs)


def _build_http_options(config: AdkConfig, types: Any) -> Any | None:
    headers: dict[str, str] = {}
    base_url = config.base_url
    api_version: str | None = None
    if _is_gemini_provider(config.provider, config.model) and base_url:
        base_url, api_version = _normalize_gemini_base_url(base_url)
    if config.base_url and config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"
    if not base_url and not headers:
        return None
    return types.HttpOptions(
        base_url=base_url,
        api_version=api_version,
        headers=headers or None,
    )


def _normalize_gemini_base_url(base_url: str) -> tuple[str, str | None]:
    normalized = base_url.rstrip("/")
    if normalized.endswith("/v1beta"):
        return normalized[: -len("/v1beta")], "v1beta"
    if normalized.endswith("/v1alpha"):
        return normalized[: -len("/v1alpha")], "v1alpha"
    return normalized, None


def _run_agent(
    runner: Any, config: AdkConfig, content: Any, session_id: str
) -> tuple[str, "TokenUsage"]:
    response_text = ""
    usage = TokenUsage()
    last_event_summary: str | None = None
    for event in runner.run(
        user_id=config.user_id,
        session_id=session_id,
        new_message=content,
    ):
        usage.update_from_event(event)
        last_event_summary = _summarize_event(event)
        if getattr(event, "is_final_response")() and getattr(event, "content", None):
            parts = getattr(event.content, "parts", [])
            if parts:
                text_parts: list[str] = []
                for part in parts:
                    text = getattr(part, "text", None)
                    thought = getattr(part, "thought", False)
                    if isinstance(text, str) and not thought:
                        text_parts.append(text)
                if text_parts:
                    response_text = "".join(text_parts)
    if not response_text:
        if last_event_summary:
            raise ValueError(
                f"ADK returned no response content; last_event={last_event_summary}"
            )
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


def _run_async(coro: Coroutine[Any, Any, Any]) -> None:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(coro)
        return
    raise RuntimeError(
        "ADK session setup requires a sync context. "
        "Call AdkCypherTranslator from a non-async entrypoint."
    )


_CODE_FENCE_RE = re.compile(r"^```(?:[A-Za-z0-9_-]+)?\s*\n?(.*?)\n?```\s*$", re.DOTALL)


def _strip_code_fences(text: str) -> str:
    stripped = text.strip()
    match = _CODE_FENCE_RE.match(stripped)
    if match:
        return match.group(1).strip()
    if "```" in stripped:
        match = re.search(
            r"```(?:[A-Za-z0-9_-]+)?\s*\n?(.*?)\n?```", stripped, re.DOTALL
        )
        if match:
            return match.group(1).strip()
    return text


def _summarize_event(event: Any) -> str:
    pieces: list[str] = [f"type={type(event).__name__}"]
    is_final = getattr(event, "is_final_response", None)
    if callable(is_final):
        try:
            pieces.append(f"final={is_final()}")
        except Exception:
            pieces.append("final=<error>")
    elif isinstance(is_final, bool):
        pieces.append(f"final={is_final}")

    finish_reason = getattr(event, "finish_reason", None)
    if finish_reason is not None:
        pieces.append(f"finish_reason={finish_reason}")

    error = getattr(event, "error", None)
    if error is not None:
        pieces.append(f"error={error}")

    error_message = getattr(event, "error_message", None)
    if error_message is not None:
        pieces.append(f"error_message={error_message}")

    content = getattr(event, "content", None)
    if content is not None:
        parts = getattr(content, "parts", None)
        if isinstance(parts, list):
            pieces.append(f"parts={len(parts)}")
            if parts:
                text = getattr(parts[0], "text", None)
                if isinstance(text, str):
                    snippet = text.strip().replace("\n", " ")
                    if len(snippet) > 160:
                        snippet = snippet[:157] + "..."
                    pieces.append(f"text_snippet={snippet!r}")

    return " ".join(pieces)


def _dump_response_text(
    response_text: str, reason: str, attempt: int, session_id: str
) -> str | None:
    dump_dir = os.environ.get("ADK_RESPONSE_DUMP_DIR")
    if not dump_dir:
        return None
    os.makedirs(dump_dir, exist_ok=True)
    safe_reason = re.sub(r"[^a-zA-Z0-9_-]+", "_", reason).strip("_")
    filename = f"adk_response_{safe_reason}_a{attempt}_{session_id}.txt"
    path = os.path.join(dump_dir, filename)
    try:
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(response_text)
    except OSError:
        return None
    return path


class TokenUsage:
    def __init__(self) -> None:
        self.prompt_tokens: int | None = None
        self.output_tokens: int | None = None
        self.total_tokens: int | None = None
        self._logger = logging.getLogger(__name__)

    def update_from_event(self, event: Any) -> None:
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


def _read_usage_value(usage: Any, keys: list[str]) -> int | None:
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


def _is_context_length_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(
        needle in message
        for needle in (
            "context_length_exceeded",
            "context length",
            "input tokens exceed",
            "maximum context",
        )
    )
