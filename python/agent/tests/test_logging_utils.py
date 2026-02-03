import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.logging_utils import format_java_like


def _raise_error() -> None:
    raise ValueError("boom")


class TestLoggingUtils(unittest.TestCase):
    def test_format_java_like_includes_thread_and_location(self) -> None:
        try:
            _raise_error()
        except Exception as exc:
            output = format_java_like(exc, thread_name="worker-1")
        self.assertIn('Exception in thread "worker-1"', output)
        self.assertIn("ValueError", output)
        filename = os.path.basename(__file__)
        self.assertIn(f"{filename}:", output)
        self.assertIn("at _raise_error", output)

    def test_format_java_like_without_thread_name(self) -> None:
        try:
            _raise_error()
        except Exception as exc:
            output = format_java_like(exc)
        self.assertTrue(output.startswith("Exception (ValueError):"))


if __name__ == "__main__":
    unittest.main()
