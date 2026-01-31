import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from k8s_graph_agent.agent import GraphAgent, GraphMcpClient
from k8s_graph_agent.mcp_client import McpClient
from k8s_graph_agent.synthesize import SimpleResponseSynthesizer
from k8s_graph_agent.translate import PrefixCypherTranslator


class FakeMcp(McpClient):
    def __init__(self) -> None:
        self.calls = []

    def initialize(self):
        return {}

    def list_tools(self):
        return []

    def list_prompts(self):
        return []

    def get_prompt(self, name, arguments=None):
        return {}

    def call_tool(self, name, arguments=None):
        self.calls.append((name, arguments))
        return {"content": [{"type": "text", "text": '[{"pod": "a"}]'}]}


class TestGraphAgent(unittest.TestCase):
    def test_answer_runs_query(self) -> None:
        fake = FakeMcp()
        graph = GraphMcpClient(mcp=fake)
        agent = GraphAgent(
            graph=graph,
            translator=PrefixCypherTranslator(),
            synthesizer=SimpleResponseSynthesizer(),
        )
        answer = agent.answer("cypher: MATCH (n) RETURN n LIMIT 1")
        self.assertTrue(answer.cypher.startswith("MATCH"))
        self.assertEqual(fake.calls[0][0], "execute_cypher_query")
        self.assertEqual(fake.calls[0][1]["query"], "MATCH (n) RETURN n LIMIT 1")
        self.assertIn("Returned", answer.response)


if __name__ == "__main__":
    unittest.main()
