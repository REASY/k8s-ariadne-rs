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


def test_answer_runs_query() -> None:
    fake = FakeMcp()
    graph = GraphMcpClient(mcp=fake)
    agent = GraphAgent(
        graph=graph,
        translator=PrefixCypherTranslator(),
        synthesizer=SimpleResponseSynthesizer(),
    )
    answer = agent.answer("cypher: MATCH (n) RETURN n LIMIT 1")
    assert answer.cypher.startswith("MATCH")
    assert fake.calls[0][0] == "execute_cypher_query"
    assert fake.calls[0][1]["query"] == "MATCH (n) RETURN n LIMIT 1"
    assert "Returned" in answer.response
