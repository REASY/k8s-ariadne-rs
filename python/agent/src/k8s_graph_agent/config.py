from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class AgentConfig:
    mcp_url: str
    request_timeout_seconds: float
    client_name: str
    client_version: str
    mcp_auth_token: str | None = None

    @classmethod
    def from_env(cls) -> "AgentConfig":
        return cls(
            mcp_url=os.environ.get("MCP_URL", "http://localhost:8080/mcp"),
            request_timeout_seconds=float(os.environ.get("MCP_TIMEOUT_SECONDS", "30")),
            client_name=os.environ.get("MCP_CLIENT_NAME", "k8s-graph-agent"),
            client_version=os.environ.get("MCP_CLIENT_VERSION", "0.1.0"),
            mcp_auth_token=os.environ.get("MCP_AUTH_TOKEN"),
        )


@dataclass(frozen=True)
class AdkConfig:
    model: str
    provider: str | None
    base_url: str | None
    api_key: str | None
    app_name: str
    user_id: str
    session_id: str
    temperature: float
    max_output_tokens: int
    use_mcp_prompt: bool

    @classmethod
    def from_env(cls) -> "AdkConfig":
        model = os.environ.get("LLM_MODEL") or os.environ.get(
            "ADK_MODEL", "gemini-2.0-flash"
        )
        provider = _normalize_provider(os.environ.get("LLM_PROVIDER"))
        provider = provider or _infer_provider(model)
        base_url = os.environ.get("LLM_BASE_URL")
        if base_url is None and provider in {"openai", "openai-compatible"}:
            base_url = os.environ.get("OPENAI_BASE_URL")
        if base_url is None and provider in {"google", "gemini"}:
            base_url = os.environ.get("GOOGLE_GEMINI_BASE_URL")

        api_key = None
        if provider in {"openai", "openai-compatible"}:
            api_key = os.environ.get("OPENAI_API_KEY")
        elif provider in {"google", "gemini"}:
            api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get(
                "GOOGLE_API_KEY"
            )

        temperature = float(os.environ.get("ADK_TEMPERATURE", "0.2"))
        temperature = _coerce_temperature(model, provider, temperature)

        return cls(
            model=model,
            provider=provider,
            base_url=base_url,
            api_key=api_key,
            app_name=os.environ.get("ADK_APP_NAME", "k8s-graph-agent"),
            user_id=os.environ.get("ADK_USER_ID", "local-user"),
            session_id=os.environ.get("ADK_SESSION_ID", "local-session"),
            temperature=temperature,
            max_output_tokens=int(os.environ.get("ADK_MAX_OUTPUT_TOKENS", "24576")),
            use_mcp_prompt=os.environ.get("ADK_USE_MCP_PROMPT", "true").lower()
            in {"1", "true", "yes"},
        )


def _infer_provider(model: str) -> str | None:
    lowered = model.strip().lower()
    if "/" in lowered:
        return _normalize_provider(lowered.split("/", 1)[0])
    if lowered.startswith("gemini"):
        return "gemini"
    if lowered.startswith(("gpt", "o1", "o3", "o4")):
        return "openai"
    return None


def _normalize_provider(provider: str | None) -> str | None:
    if provider is None:
        return None
    lowered = provider.strip().lower()
    if lowered in {"gemini", "google"}:
        return "gemini"
    return lowered


def _coerce_temperature(
    model: str, provider: str | None, temperature: float
) -> float:
    normalized = model.strip().lower()
    if "/" in normalized:
        normalized = normalized.split("/", 1)[1]
    if normalized.startswith("gpt-5") and (provider in {None, "openai"}):
        return 1.0
    return temperature
