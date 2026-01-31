from __future__ import annotations

from dataclasses import dataclass
import logging

from .mcp_client import McpClient, extract_json_content
from .models import AgentAnswer, JsonValue
from .synthesize import ResponseSynthesizer
from .translate import CypherTranslator


@dataclass
class GraphMcpClient:
    mcp: McpClient

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def execute_cypher(self, query: str) -> JsonValue:
        self._logger.debug("executing cypher via MCP")
        result = self.mcp.call_tool("execute_cypher_query", {"query": query})
        return extract_json_content(result)


@dataclass
class GraphAgent:
    graph: GraphMcpClient
    translator: CypherTranslator
    synthesizer: ResponseSynthesizer

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def answer(self, question: str) -> AgentAnswer:
        self._logger.info("answering question")
        cypher = self.translator.translate(question)
        self._logger.info("cypher generated")
        result = self.graph.execute_cypher(cypher.text)
        if isinstance(result, list):
            self._logger.info("query returned %d rows", len(result))
        response = self.synthesizer.synthesize(question, cypher.text, result)
        return AgentAnswer(
            question=question, cypher=cypher.text, result=result, response=response
        )
