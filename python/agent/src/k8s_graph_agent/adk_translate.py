from __future__ import annotations

from dataclasses import dataclass, field
import asyncio
import json
import logging
import os
from pathlib import Path
import re
import uuid
from typing import Any, Coroutine

from pydantic import BaseModel, Field, ValidationError
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from .config import AdkConfig
from .cypher_validator import CypherSchemaValidator, CypherValidationError
from .graph_schema import GraphSchema
from .mcp_client import McpClient, extract_json_content
from .models import CypherQuery
from .prompting import (
    prompt_sections_from_graph_schema_payload,
    render_prompt_bundle,
)


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


class CypherTranslationStrict(BaseModel):
    cypher: str = Field(..., description="Cypher query to run against the graph")
    notes: str | None = Field(
        ...,
        description="Clarifications or assumptions about the query (nullable).",
    )
    confidence: float | None = Field(
        ...,
        description="Confidence score from 0.0 to 1.0 (nullable).",
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
    _prompt_bundle_text: str | None = field(init=False, default=None)
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
        prompt_text = self._build_prompt_text(question)
        return self._translate_from_prompt(
            question=question,
            prompt_text=prompt_text,
            max_attempts=max_attempts,
        )

    def translate_with_execution_error(
        self, question: str, cypher: str, error: str, max_attempts: int = 1
    ) -> TranslationOutcome:
        prompt_text = self._build_prompt_text(question)
        retry_prompt = _build_retry_prompt(prompt_text, cypher, error)
        return self._translate_from_prompt(
            question=question,
            prompt_text=retry_prompt,
            max_attempts=max_attempts,
        )

    def _build_prompt_text(self, question: str) -> str:
        prompt_text = question
        if self.config.prompt_bundle_file:
            prompt_text = render_prompt_bundle(self._load_prompt_bundle(), question)
            self._logger.debug(
                "using prompt bundle override from %s",
                self.config.prompt_bundle_file,
            )
        elif self.config.use_mcp_prompt:
            try:
                schema_result = self.mcp.call_tool(
                    "graph_schema", {"format": "structured"}
                )
                payload = extract_json_content(schema_result)
                if isinstance(payload, dict):
                    sections = prompt_sections_from_graph_schema_payload(payload)
                    if sections is not None:
                        bundle_text = "\n\n".join(
                            [
                                sections.instruction,
                                sections.rules,
                                sections.schema_reference,
                                sections.node_connectivity,
                                sections.footer,
                            ]
                        )
                        prompt_text = render_prompt_bundle(bundle_text, question)
                        self._logger.debug("using graph_schema-derived prompt bundle")
            except Exception:
                self._logger.debug(
                    "graph_schema prompt fallback unavailable; using raw question",
                    exc_info=True,
                )
        return prompt_text

    def _translate_from_prompt(
        self, question: str, prompt_text: str, max_attempts: int
    ) -> TranslationOutcome:
        runner: tuple[Any, Any] | None = None
        types: Any | None = None
        use_direct_openai_http = _should_use_direct_openai_http(self.config)
        if not use_direct_openai_http:
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
            try:
                if use_direct_openai_http:
                    response_text, usage = _run_openai_http_completion(
                        config=self.config,
                        prompt_text=current_prompt,
                    )
                else:
                    assert runner is not None
                    assert types is not None
                    content = types.Content(
                        role="user", parts=[types.Part(text=current_prompt)]
                    )
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

        enforce_json = use_native_gemini or (
            _is_openai_provider(self.config.provider, self.config.model)
            and _supports_openai_json_schema(self.config.provider, self.config.model)
        )
        if enforce_json and _is_openai_provider(
            self.config.provider, self.config.model
        ):
            instruction = (
                "You translate questions about a Kubernetes graph into a single Cypher query. "
                "Always respect the schema and query rules included in the prompt. "
                "Return only JSON with keys: cypher, notes, confidence. "
                "If notes or confidence are unknown, set them to null."
            )
            output_schema = CypherTranslationStrict
        else:
            instruction = (
                "You translate questions about a Kubernetes graph into a single Cypher query. "
                "Always respect the schema and query rules included in the prompt. "
                "Return only JSON with keys: cypher (string), optional notes (string), "
                "optional confidence (number between 0 and 1)."
            )
            output_schema = CypherTranslation if enforce_json else None
        generate_config = _build_generate_content_config(self.config, types)
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

    def _load_prompt_bundle(self) -> str:
        if self._prompt_bundle_text is not None:
            return self._prompt_bundle_text
        bundle_file = self.config.prompt_bundle_file
        if not bundle_file:
            raise ValueError("prompt bundle file is not configured")
        bundle_path = Path(bundle_file)
        self._prompt_bundle_text = bundle_path.read_text(encoding="utf-8")
        return self._prompt_bundle_text


def _format_model(model: str, provider: str | None) -> str:
    normalized = model.strip()
    if "/" in normalized:
        return normalized
    if provider:
        if provider.strip().lower() == "openai-compatible":
            return normalized
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


def _is_openai_provider(provider: str | None, model: str) -> bool:
    if provider and provider.strip().lower() in {"openai", "openai-compatible"}:
        return True
    normalized = model.strip().lower()
    return normalized.startswith(("openai/", "gpt-", "o1", "o3", "o4", "chatgpt"))


def _is_deepseek_provider(provider: str | None, model: str) -> bool:
    if provider and provider.strip().lower() == "deepseek":
        return True
    normalized = model.strip().lower()
    return normalized.startswith(("deepseek", "openai/deepseek", "deepseek/"))


def _supports_openai_json_schema(provider: str | None, model: str) -> bool:
    raw = os.environ.get("K8S_GRAPH_DISABLE_OPENAI_JSON_SCHEMA") or os.environ.get(
        "ADK_DISABLE_OPENAI_JSON_SCHEMA"
    )
    if raw and raw.strip().lower() in {"1", "true", "yes"}:
        return False
    normalized = model.strip().lower()
    if "deepseek" in normalized:
        return False
    return _is_openai_provider(provider, model)


def _should_use_direct_openai_http(config: AdkConfig) -> bool:
    if not config.base_url:
        return False
    if not _is_openai_provider(config.provider, config.model):
        return False
    if (config.provider or "").strip().lower() == "openai-compatible":
        return True
    normalized = config.model.strip().lower()
    if "/" in normalized:
        return False
    return not normalized.startswith(("gpt-", "o1", "o3", "o4", "chatgpt"))


def _run_openai_http_completion(
    *, config: AdkConfig, prompt_text: str
) -> tuple[str, "TokenUsage"]:
    import httpx

    model_name = _strip_provider_prefix(config.model)
    base_url = _normalize_openai_http_base_url(config.base_url)
    headers = {"Content-Type": "application/json"}
    if config.api_key:
        headers["Authorization"] = f"Bearer {config.api_key}"

    payload: dict[str, Any] = {
        "model": model_name,
        "messages": [{"role": "user", "content": prompt_text}],
        "temperature": config.temperature,
        "max_tokens": config.max_output_tokens,
    }
    if _supports_openai_json_schema(config.provider, config.model):
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "cypher_translation",
                "strict": True,
                "schema": CypherTranslationStrict.model_json_schema(),
            },
        }

    timeout = httpx.Timeout(connect=10.0, read=300.0, write=300.0, pool=300.0)
    with httpx.Client(timeout=timeout) as client:
        response = client.post(
            f"{base_url}/chat/completions",
            headers=headers,
            json=payload,
        )
        response.raise_for_status()

    data = response.json()
    usage = TokenUsage()
    usage.update_from_usage(data.get("usage"))
    usage.log_if_present()

    choices = data.get("choices")
    if not isinstance(choices, list) or not choices:
        raise ValueError(
            f"OpenAI-compatible endpoint returned no choices: {json.dumps(data)[:400]}"
        )
    message = choices[0].get("message")
    if not isinstance(message, dict):
        raise ValueError(
            f"OpenAI-compatible endpoint returned no message: {json.dumps(data)[:400]}"
        )
    content = message.get("content")
    if isinstance(content, str) and content.strip():
        return content, usage
    raise ValueError(
        f"OpenAI-compatible endpoint returned no message content: {json.dumps(data)[:400]}"
    )


def _normalize_openai_http_base_url(base_url: str | None) -> str:
    if not base_url:
        raise ValueError("OpenAI-compatible HTTP path requires a base_url")
    normalized = base_url.rstrip("/")
    if normalized.endswith("/v1"):
        return normalized
    return f"{normalized}/v1"


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


def _gemini_request_retries(config: AdkConfig) -> int:
    if not _is_gemini_provider(config.provider, config.model):
        return 0
    raw = os.environ.get("K8S_GRAPH_GEMINI_REQUEST_RETRIES") or os.environ.get(
        "ADK_GEMINI_REQUEST_RETRIES"
    )
    if raw is None:
        return 2
    try:
        return max(0, int(raw))
    except ValueError:
        return 2


def _gemini_request_retry_base_seconds() -> float:
    raw = os.environ.get(
        "K8S_GRAPH_GEMINI_REQUEST_RETRY_BASE_SECONDS"
    ) or os.environ.get("ADK_GEMINI_REQUEST_RETRY_BASE_SECONDS")
    if raw is None:
        return 1.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 1.0


def _rate_limit_request_retries(config: AdkConfig) -> int:
    raw = os.environ.get("K8S_GRAPH_RATE_LIMIT_RETRIES") or os.environ.get(
        "ADK_RATE_LIMIT_RETRIES"
    )
    if raw is not None:
        try:
            return max(0, int(raw))
        except ValueError:
            return 0
    if _is_deepseek_provider(config.provider, config.model):
        return 3
    return 0


def _rate_limit_request_retry_base_seconds() -> float:
    raw = os.environ.get(
        "K8S_GRAPH_RATE_LIMIT_RETRY_BASE_SECONDS"
    ) or os.environ.get("ADK_RATE_LIMIT_RETRY_BASE_SECONDS")
    if raw is None:
        return 2.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 2.0


def _rate_limit_request_retry_max_seconds() -> float:
    raw = os.environ.get(
        "K8S_GRAPH_RATE_LIMIT_RETRY_MAX_SECONDS"
    ) or os.environ.get("ADK_RATE_LIMIT_RETRY_MAX_SECONDS")
    if raw is None:
        return 20.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 20.0


def _iter_exception_chain(exc: BaseException):
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        yield current
        current = current.__cause__ or current.__context__


def _should_retry_gemini_error(config: AdkConfig, exc: Exception) -> bool:
    if not _is_gemini_provider(config.provider, config.model):
        return False
    for err in _iter_exception_chain(exc):
        status = getattr(err, "status_code", None) or getattr(err, "status", None)
        if status == 502:
            return True
        message = str(err).lower()
        if "bad gateway" in message and "502" in message:
            return True
        if (
            "unexpected end of stream" in message
            and "generativelanguage.googleapis.com" in message
        ):
            return True
    return False


def _is_rate_limit_error(exc: Exception) -> bool:
    for err in _iter_exception_chain(exc):
        status = getattr(err, "status_code", None) or getattr(err, "status", None)
        if status == 429:
            return True
        name = err.__class__.__name__.lower()
        if "ratelimit" in name or name == "ratelimiterror":
            return True
        message = str(err).lower()
        if "rate limited" in message:
            return True
        if "too many tokens" in message:
            return True
        if "too many requests" in message:
            return True
        if "retry in " in message and "deepseek-r1" in message:
            return True
    return False


def _should_retry_request_error(config: AdkConfig, exc: Exception) -> bool:
    if _should_retry_gemini_error(config, exc):
        return True
    if _is_rate_limit_error(exc):
        return True
    if _is_deepseek_provider(config.provider, config.model):
        message = str(exc).lower()
        if "adk returned no response content" in message:
            return True
    return False


def _run_agent(
    runner: Any, config: AdkConfig, content: Any, session_id: str
) -> tuple[str, "TokenUsage"]:
    def _run_once() -> tuple[str, TokenUsage]:
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
            if getattr(event, "is_final_response")() and getattr(
                event, "content", None
            ):
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

    gemini_retries = _gemini_request_retries(config)
    rate_limit_retries = _rate_limit_request_retries(config)
    max_retries = max(gemini_retries, rate_limit_retries)
    if max_retries <= 0:
        return _run_once()

    if rate_limit_retries > 0:
        base_delay = _rate_limit_request_retry_base_seconds()
        max_delay = _rate_limit_request_retry_max_seconds()
    else:
        base_delay = _gemini_request_retry_base_seconds()
        max_delay = None
    total_attempts = max_retries + 1

    def _log_retry(retry_state) -> None:
        exc = None
        if retry_state.outcome is not None:
            exc = retry_state.outcome.exception()
        delay = 0.0
        if retry_state.next_action is not None:
            delay = retry_state.next_action.sleep
        logging.getLogger(__name__).warning(
            "LLM request failed; retrying in %.1fs (%d/%d): %s",
            delay,
            retry_state.attempt_number,
            total_attempts,
            exc,
        )

    wait_kwargs: dict[str, Any] = {
        "multiplier": base_delay,
        "min": base_delay,
    }
    if max_delay is not None:
        wait_kwargs["max"] = max_delay

    retrying = Retrying(
        stop=stop_after_attempt(total_attempts),
        wait=wait_exponential(**wait_kwargs),
        retry=retry_if_exception(lambda exc: _should_retry_request_error(config, exc)),
        reraise=True,
        before_sleep=_log_retry,
    )
    for attempt in retrying:
        with attempt:
            return _run_once()
    raise RuntimeError("Retry loop exited without a response")


def _build_retry_prompt(base_prompt: str, cypher: str, error: str) -> str:
    return (
        f"{base_prompt}\n\n"
        "The previous Cypher failed validation or execution.\n"
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

    def update_from_usage(self, usage: Any) -> None:
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

    def update_from_event(self, event: Any) -> None:
        usage = getattr(event, "usage_metadata", None)
        self.update_from_usage(usage)

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
