from k8s_graph_agent.adk_translate import (
    _is_anthropic_provider,
    _is_gemini_provider,
    _is_openai_provider,
    _supports_openai_json_schema,
    _normalize_gemini_base_url,
    _strip_code_fences,
    _strip_provider_prefix,
)


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


def test_supports_openai_json_schema() -> None:
    assert _supports_openai_json_schema("openai", "gpt-5.2-2025-12-11")
    assert not _supports_openai_json_schema("openai", "deepseek-r1")
    assert not _supports_openai_json_schema("openai-compatible", "openai/deepseek-r1")
