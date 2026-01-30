import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.prompting import extract_prompt_text


class TestPrompting(unittest.TestCase):
    def test_extract_prompt_text(self) -> None:
        prompt_result = {
            "messages": [
                {"content": {"type": "text", "text": "hello"}},
                {"content": {"text": "world"}},
            ]
        }
        text = extract_prompt_text(prompt_result)
        self.assertEqual(text, "hello\nworld")


if __name__ == "__main__":
    unittest.main()
