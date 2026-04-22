from k8s_graph_agent.agent import GraphAgent, GraphMcpClient
from k8s_graph_agent.models import CypherQuery
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
        return {
            "structuredContent": {
                "columns": ["pod"],
                "rows": [["a"]],
                "row_count": 1,
                "truncated": False,
                "duration_ms": 1,
            }
        }


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
    assert fake.calls[0][0] == "graph_query"
    assert fake.calls[0][1]["query"] == "MATCH (n) RETURN n LIMIT 1"
    assert "Returned" in answer.response


class FlakyMcp(McpClient):
    def __init__(self) -> None:
        self.calls = []
        self._count = 0

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
        self._count += 1
        if self._count == 1:
            raise RuntimeError(
                "MemgraphError: QueryError: Query execution error: Unbound variable: ns."
            )
        return {
            "structuredContent": {
                "columns": ["pod"],
                "rows": [["b"]],
                "row_count": 1,
                "truncated": False,
                "duration_ms": 1,
            }
        }


class RepairingTranslator:
    def __init__(self) -> None:
        self.repair_calls = []

    def translate(self, question: str) -> CypherQuery:
        return CypherQuery(
            text=(
                "MATCH (ns:Namespace) "
                "MATCH (pvc:PersistentVolumeClaim)-[:BelongsTo]->(ns) "
                "OPTIONAL MATCH (pvc)-[:BoundTo]->(pv:PersistentVolume) "
                "WITH pvc, pv "
                "RETURN pvc['metadata']['name'], ns['metadata']['name']"
            )
        )

    def translate_with_execution_error(
        self, question: str, cypher: str, error: str, max_attempts: int = 1
    ):
        self.repair_calls.append((question, cypher, error, max_attempts))
        return type("Outcome", (), {"cypher": "MATCH (n) RETURN n LIMIT 1", "error": None})()


def test_answer_retries_once_after_execution_error() -> None:
    fake = FlakyMcp()
    translator = RepairingTranslator()
    graph = GraphMcpClient(mcp=fake)
    agent = GraphAgent(
        graph=graph,
        translator=translator,
        synthesizer=SimpleResponseSynthesizer(),
    )

    answer = agent.answer("List PVCs with namespaces")

    assert len(fake.calls) == 2
    assert fake.calls[0][1]["query"].startswith("MATCH (ns:Namespace)")
    assert fake.calls[1][1]["query"] == "MATCH (n) RETURN n LIMIT 1"
    assert translator.repair_calls[0][0] == "List PVCs with namespaces"
    assert "Unbound variable: ns." in translator.repair_calls[0][2]
    assert answer.cypher == "MATCH (n) RETURN n LIMIT 1"
