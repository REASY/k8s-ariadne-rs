from __future__ import annotations

from dataclasses import dataclass

from .mcp_client import McpClient, extract_json_content
from .models import AgentAnswer, JsonValue
from .synthesize import ResponseSynthesizer
from .translate import CypherTranslator


@dataclass
class GraphMcpClient:
    mcp: McpClient

    def execute_cypher(self, query: str) -> JsonValue:
        result = self.mcp.call_tool("execute_cypher_query", {"query": query})
        return extract_json_content(result)


@dataclass
class GraphAgent:
    graph: GraphMcpClient
    translator: CypherTranslator
    synthesizer: ResponseSynthesizer

    def answer(self, question: str) -> AgentAnswer:
        cypher = self.translator.translate(question)
        result = self.graph.execute_cypher(cypher.text)
        response = self.synthesizer.synthesize(question, cypher.text, result)
        return AgentAnswer(
            question=question, cypher=cypher.text, result=result, response=response
        )
