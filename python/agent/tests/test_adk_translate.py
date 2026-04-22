from k8s_graph_agent.adk_translate import AdkCypherTranslator, TranslationOutcome, TokenUsage
from k8s_graph_agent.adk_translate import (
    _format_model,
    _is_deepseek_provider,
    _is_rate_limit_error,
    _is_anthropic_provider,
    _is_gemini_provider,
    _is_openai_provider,
    _normalize_openai_http_base_url,
    _should_use_direct_openai_http,
    _run_agent,
    _should_retry_request_error,
    _supports_openai_json_schema,
    _normalize_gemini_base_url,
    _strip_code_fences,
    _strip_provider_prefix,
)
from k8s_graph_agent.config import AdkConfig


def test_pass_through_plain_json() -> None:
    text = '{"cypher":"MATCH (n) RETURN n"}'
    assert _strip_code_fences(text) == text


def test_strips_json_fence() -> None:
    text = '```json\n{"cypher":"MATCH (n) RETURN n"}\n```'
    assert _strip_code_fences(text) == '{"cypher":"MATCH (n) RETURN n"}'


def test_strips_plain_fence() -> None:
    text = '```\n{"cypher":"MATCH (n) RETURN n"}\n```'
    assert _strip_code_fences(text) == '{"cypher":"MATCH (n) RETURN n"}'


def test_extracts_first_fence() -> None:
    text = 'Here is the result:\n```json\n{"cypher":"MATCH (n) RETURN n"}\n```'
    assert _strip_code_fences(text) == '{"cypher":"MATCH (n) RETURN n"}'


def test_strip_provider_prefix() -> None:
    assert _strip_provider_prefix("gemini/gemini-2.5-pro") == "gemini-2.5-pro"
    assert _strip_provider_prefix("gemini-2.5-pro") == "gemini-2.5-pro"


def test_format_model_leaves_openai_compatible_bare() -> None:
    assert _format_model("gemma-4-31b-it", "openai-compatible") == "gemma-4-31b-it"
    assert _format_model("gpt-5.4-2026-03-05", "openai") == "openai/gpt-5.4-2026-03-05"


def test_is_gemini_provider() -> None:
    assert _is_gemini_provider("gemini", "gemini-2.5-pro")
    assert _is_gemini_provider(None, "gemini-2.5-pro")
    assert _is_gemini_provider(None, "gemini/gemini-2.5-pro")
    assert not _is_gemini_provider("openai", "gpt-5.2")


def test_normalize_gemini_base_url() -> None:
    base, version = _normalize_gemini_base_url(
        "https://genai-gateway.agoda.is/gemini/v1beta"
    )
    assert base == "https://genai-gateway.agoda.is/gemini"
    assert version == "v1beta"
    base, version = _normalize_gemini_base_url(
        "https://genai-gateway.agoda.is/gemini/v1alpha/"
    )
    assert base == "https://genai-gateway.agoda.is/gemini"
    assert version == "v1alpha"
    base, version = _normalize_gemini_base_url("https://genai-gateway.agoda.is/gemini")
    assert base == "https://genai-gateway.agoda.is/gemini"
    assert version is None


def test_is_anthropic_provider() -> None:
    assert _is_anthropic_provider("anthropic", "gpt-5.2-2025-12-11")
    assert _is_anthropic_provider("claude", "gpt-5.2-2025-12-11")
    assert not _is_anthropic_provider("openai", "claude-sonnet-4-20250514")
    assert _is_anthropic_provider(None, "claude-sonnet-4-20250514")
    assert _is_anthropic_provider(None, "anthropic/claude-3-5-haiku-20241022-v1")
    assert _is_anthropic_provider(None, "claude/claude-3-5-haiku-20241022-v1")
    assert not _is_anthropic_provider(None, "openai/claude-sonnet-4-20250514")


def test_is_openai_provider() -> None:
    assert _is_openai_provider("openai", "claude-sonnet-4-20250514")
    assert _is_openai_provider("openai-compatible", "deepseek-r1")
    assert _is_openai_provider(None, "openai/gpt-5.2")
    assert _is_openai_provider(None, "gpt-5.2-2025-12-11")
    assert _is_openai_provider("anthropic", "gpt-5.2")


def test_is_deepseek_provider() -> None:
    assert _is_deepseek_provider("deepseek", "gpt-5.2")
    assert _is_deepseek_provider(None, "deepseek-r1")
    assert _is_deepseek_provider(None, "openai/deepseek-r1")
    assert not _is_deepseek_provider("openai", "gpt-5.4")


def test_supports_openai_json_schema() -> None:
    assert _supports_openai_json_schema("openai", "gpt-5.2-2025-12-11")
    assert not _supports_openai_json_schema("openai", "deepseek-r1")
    assert not _supports_openai_json_schema("openai-compatible", "openai/deepseek-r1")


def test_normalize_openai_http_base_url() -> None:
    assert _normalize_openai_http_base_url("http://127.0.0.1:1234") == "http://127.0.0.1:1234/v1"
    assert _normalize_openai_http_base_url("http://127.0.0.1:1234/v1") == "http://127.0.0.1:1234/v1"


def test_should_use_direct_openai_http_for_local_openai_compatible_model() -> None:
    local = AdkConfig(
        model="gemma-4-31b-it",
        provider="openai",
        base_url="http://127.0.0.1:1234",
        api_key=None,
        app_name="app",
        user_id="user",
        session_id="session",
        temperature=0.2,
        max_output_tokens=1024,
        use_mcp_prompt=False,
    )
    hosted = AdkConfig(
        model="openai/gpt-5.4-2026-03-05",
        provider="openai",
        base_url="https://gateway.example/v1",
        api_key="key",
        app_name="app",
        user_id="user",
        session_id="session",
        temperature=0.2,
        max_output_tokens=1024,
        use_mcp_prompt=False,
    )

    assert _should_use_direct_openai_http(local)
    assert not _should_use_direct_openai_http(hosted)


class _FakeRateLimitError(Exception):
    status_code = 429


def test_is_rate_limit_error_detects_common_forms() -> None:
    assert _is_rate_limit_error(_FakeRateLimitError("rate limited"))
    assert _is_rate_limit_error(RuntimeError("Too many tokens, please wait before trying again."))
    assert not _is_rate_limit_error(RuntimeError("Unbound variable: ns."))


def test_should_retry_request_error_retries_deepseek_no_response() -> None:
    config = AdkConfig(
        model="deepseek-r1",
        provider="openai-compatible",
        base_url="https://genai-gateway.agoda.is/v1",
        api_key="key",
        app_name="app",
        user_id="user",
        session_id="session",
        temperature=0.2,
        max_output_tokens=1024,
        use_mcp_prompt=False,
    )
    assert _should_retry_request_error(
        config, ValueError("ADK returned no response content")
    )


class _FakePart:
    def __init__(self, text: str | None) -> None:
        self.text = text
        self.thought = False


class _FakeContent:
    def __init__(self, text: str) -> None:
        self.parts = [_FakePart(text)]


class _FakeEvent:
    def __init__(self, text: str) -> None:
        self.content = _FakeContent(text)
        self.usage_metadata = None

    def is_final_response(self) -> bool:
        return True


class _FlakyRunner:
    def __init__(self) -> None:
        self.calls = 0

    def run(self, **kwargs):
        self.calls += 1
        if self.calls < 3:
            raise _FakeRateLimitError("rate limited for deepseek-r1")
        yield _FakeEvent('{"cypher":"MATCH (n) RETURN n"}')


def test_run_agent_retries_rate_limit_then_succeeds(monkeypatch) -> None:
    monkeypatch.setenv("K8S_GRAPH_RATE_LIMIT_RETRIES", "3")
    monkeypatch.setenv("K8S_GRAPH_RATE_LIMIT_RETRY_BASE_SECONDS", "0")
    config = AdkConfig(
        model="deepseek-r1",
        provider="openai-compatible",
        base_url="https://genai-gateway.agoda.is/v1",
        api_key="key",
        app_name="app",
        user_id="user",
        session_id="session",
        temperature=0.2,
        max_output_tokens=1024,
        use_mcp_prompt=False,
    )
    runner = _FlakyRunner()

    response, usage = _run_agent(
        runner=runner,
        config=config,
        content=object(),
        session_id="s1",
    )

    assert response == '{"cypher":"MATCH (n) RETURN n"}'
    assert runner.calls == 3
    assert usage.total_tokens is None


class _FakeMcp:
    def get_prompt(self, name, arguments=None):
        return {}


def test_translate_with_execution_error_builds_retry_prompt(monkeypatch) -> None:
    translator = AdkCypherTranslator(
        mcp=_FakeMcp(),
        config=AdkConfig(
            model="gemini-2.5-flash",
            provider="gemini",
            base_url=None,
            api_key=None,
            app_name="app",
            user_id="user",
            session_id="session",
            temperature=0.2,
            max_output_tokens=1024,
            use_mcp_prompt=False,
        ),
    )

    captured = {}

    def fake_build_prompt_text(question: str) -> str:
        assert question == "List PVCs"
        return "BASE PROMPT"

    def fake_translate_from_prompt(question: str, prompt_text: str, max_attempts: int):
        captured["question"] = question
        captured["prompt_text"] = prompt_text
        captured["max_attempts"] = max_attempts
        return TranslationOutcome(
            cypher="MATCH (n) RETURN n",
            attempts=[],
            total_usage=TokenUsage(),
            error=None,
        )

    monkeypatch.setattr(translator, "_build_prompt_text", fake_build_prompt_text)
    monkeypatch.setattr(translator, "_translate_from_prompt", fake_translate_from_prompt)

    outcome = translator.translate_with_execution_error(
        "List PVCs",
        "MATCH (bad) RETURN bad",
        "Unbound variable: ns.",
        max_attempts=1,
    )

    assert outcome.cypher == "MATCH (n) RETURN n"
    assert captured["question"] == "List PVCs"
    assert captured["max_attempts"] == 1
    assert "BASE PROMPT" in captured["prompt_text"]
    assert "Previous Cypher:\nMATCH (bad) RETURN bad" in captured["prompt_text"]
    assert "Error: Unbound variable: ns." in captured["prompt_text"]
