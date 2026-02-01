import unittest

from k8s_graph_agent.adk_translate import (
    _is_gemini_provider,
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


if __name__ == "__main__":
    unittest.main()
