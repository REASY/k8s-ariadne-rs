import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.mcp_client import (
    _parse_sse_messages,
    _pick_response,
    extract_json_content,
)


class TestMcpClientHelpers(unittest.TestCase):
    def test_parse_sse_messages(self) -> None:
        body = 'data: {"jsonrpc":"2.0","id":1,"result":{}}\n\n'
        messages = _parse_sse_messages(body)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["id"], 1)

    def test_pick_response(self) -> None:
        responses = [
            {"id": 1, "result": {"value": "a"}},
            {"id": 2, "result": {"value": "b"}},
        ]
        picked = _pick_response(responses, 2)
        self.assertIsNotNone(picked)
        self.assertEqual(picked["result"]["value"], "b")

    def test_extract_json_content(self) -> None:
        tool_result = {
            "content": [
                {"type": "text", "text": '[{"pod": "a"}]'},
            ]
        }
        extracted = extract_json_content(tool_result)
        self.assertIsInstance(extracted, list)
        self.assertEqual(extracted[0]["pod"], "a")


if __name__ == "__main__":
    unittest.main()
