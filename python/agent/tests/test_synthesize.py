import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.synthesize import SreResponseSynthesizer


class TestSreResponseSynthesizer(unittest.TestCase):
    def test_empty_result(self) -> None:
        synthesizer = SreResponseSynthesizer()
        response = synthesizer.synthesize("why?", "MATCH (n) RETURN n", [])
        self.assertIn("Rows returned: 0", response)
        self.assertIn("Next steps:", response)


if __name__ == "__main__":
    unittest.main()
