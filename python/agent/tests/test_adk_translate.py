import unittest

from k8s_graph_agent.adk_translate import (
    _is_anthropic_provider,
    _is_gemini_provider,
    _is_openai_provider,
    _supports_openai_json_schema,
    _normalize_gemini_base_url,
    _strip_code_fences,
    _strip_provider_prefix,
)


class TestStripCodeFences(unittest.TestCase):
    def test_pass_through_plain_json(self) -> None:
        text = '{"cypher":"MATCH (n) RETURN n"}'
        self.assertEqual(_strip_code_fences(text), text)

    def test_strips_json_fence(self) -> None:
        text = '```json\n{"cypher":"MATCH (n) RETURN n"}\n```'
        self.assertEqual(_strip_code_fences(text), '{"cypher":"MATCH (n) RETURN n"}')

    def test_strips_plain_fence(self) -> None:
        text = '```\n{"cypher":"MATCH (n) RETURN n"}\n```'
        self.assertEqual(_strip_code_fences(text), '{"cypher":"MATCH (n) RETURN n"}')

    def test_extracts_first_fence(self) -> None:
        text = 'Here is the result:\n```json\n{"cypher":"MATCH (n) RETURN n"}\n```'
        self.assertEqual(_strip_code_fences(text), '{"cypher":"MATCH (n) RETURN n"}')


class TestGeminiHelpers(unittest.TestCase):
    def test_strip_provider_prefix(self) -> None:
        self.assertEqual(
            _strip_provider_prefix("gemini/gemini-2.5-pro"), "gemini-2.5-pro"
        )
        self.assertEqual(_strip_provider_prefix("gemini-2.5-pro"), "gemini-2.5-pro")

    def test_is_gemini_provider(self) -> None:
        self.assertTrue(_is_gemini_provider("gemini", "gemini-2.5-pro"))
        self.assertTrue(_is_gemini_provider(None, "gemini-2.5-pro"))
        self.assertTrue(_is_gemini_provider(None, "gemini/gemini-2.5-pro"))
        self.assertFalse(_is_gemini_provider("openai", "gpt-5.2"))

    def test_normalize_gemini_base_url(self) -> None:
        base, version = _normalize_gemini_base_url(
            "https://genai-gateway.agoda.is/gemini/v1beta"
        )
        self.assertEqual(base, "https://genai-gateway.agoda.is/gemini")
        self.assertEqual(version, "v1beta")
        base, version = _normalize_gemini_base_url(
            "https://genai-gateway.agoda.is/gemini/v1alpha/"
        )
        self.assertEqual(base, "https://genai-gateway.agoda.is/gemini")
        self.assertEqual(version, "v1alpha")
        base, version = _normalize_gemini_base_url(
            "https://genai-gateway.agoda.is/gemini"
        )
        self.assertEqual(base, "https://genai-gateway.agoda.is/gemini")
        self.assertIsNone(version)


class TestAnthropicHelpers(unittest.TestCase):
    def test_is_anthropic_provider(self) -> None:
        self.assertTrue(_is_anthropic_provider("anthropic", "gpt-5.2-2025-12-11"))
        self.assertTrue(_is_anthropic_provider("claude", "gpt-5.2-2025-12-11"))
        self.assertFalse(_is_anthropic_provider("openai", "claude-sonnet-4-20250514"))
        self.assertTrue(_is_anthropic_provider(None, "claude-sonnet-4-20250514"))
        self.assertTrue(
            _is_anthropic_provider(None, "anthropic/claude-3-5-haiku-20241022-v1")
        )
        self.assertTrue(
            _is_anthropic_provider(None, "claude/claude-3-5-haiku-20241022-v1")
        )
        self.assertFalse(
            _is_anthropic_provider(None, "openai/claude-sonnet-4-20250514")
        )


class TestOpenaiHelpers(unittest.TestCase):
    def test_is_openai_provider(self) -> None:
        self.assertTrue(_is_openai_provider("openai", "claude-sonnet-4-20250514"))
        self.assertTrue(_is_openai_provider("openai-compatible", "deepseek-r1"))
        self.assertTrue(_is_openai_provider(None, "openai/gpt-5.2"))
        self.assertTrue(_is_openai_provider(None, "gpt-5.2-2025-12-11"))
        self.assertTrue(_is_openai_provider("anthropic", "gpt-5.2"))

    def test_supports_openai_json_schema(self) -> None:
        self.assertTrue(_supports_openai_json_schema("openai", "gpt-5.2-2025-12-11"))
        self.assertFalse(_supports_openai_json_schema("openai", "deepseek-r1"))
        self.assertFalse(
            _supports_openai_json_schema("openai-compatible", "openai/deepseek-r1")
        )


if __name__ == "__main__":
    unittest.main()
