import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.graph_schema import GraphSchema


class TestGraphSchema(unittest.TestCase):
    def test_from_payload(self) -> None:
        payload = {
            "relationships": [
                {"from": "Host", "edge": "IsClaimedBy", "to": "Ingress"},
                {"from": "Ingress", "edge": "DefinesBackend", "to": "IngressServiceBackend"},
            ]
        }
        schema = GraphSchema.from_payload(payload)
        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertTrue(schema.allows("IsClaimedBy", "Host", "Ingress"))
        self.assertTrue(schema.allows("DefinesBackend", "Ingress", "IngressServiceBackend"))
        self.assertFalse(schema.allows("DefinesBackend", "Host", "IngressServiceBackend"))


if __name__ == "__main__":
    unittest.main()
