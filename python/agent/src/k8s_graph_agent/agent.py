from __future__ import annotations

from dataclasses import dataclass, field
import logging

from .mcp_client import McpClient, extract_json_content, normalize_graph_query_payload
from .models import AgentAnswer, CypherQuery, JsonValue
from .synthesize import ResponseSynthesizer
from .translate import CypherTranslator


@dataclass
class GraphMcpClient:
    mcp: McpClient
    _logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def execute_cypher(self, query: str) -> JsonValue:
        self._logger.debug("executing cypher via MCP")
        result = self.mcp.call_tool("graph_query", {"query": query})
        return normalize_graph_query_payload(extract_json_content(result))


@dataclass
class GraphAgent:
    graph: GraphMcpClient
    translator: CypherTranslator
    synthesizer: ResponseSynthesizer
    _logger: logging.Logger = field(init=False)

    def __post_init__(self) -> None:
        self._logger = logging.getLogger(__name__)

    def answer(self, question: str) -> AgentAnswer:
        self._logger.info("answering question")
        cypher = self.translator.translate(question)
        self._logger.info("cypher generated")
        try:
            result = self.graph.execute_cypher(cypher.text)
        except Exception as exc:
            repaired = self._retry_after_execution_error(question, cypher.text, exc)
            if repaired is None:
                raise
            cypher = repaired
            result = self.graph.execute_cypher(cypher.text)
        if isinstance(result, list):
            self._logger.info("query returned %d rows", len(result))
        response = self.synthesizer.synthesize(question, cypher.text, result)
        return AgentAnswer(
            question=question, cypher=cypher.text, result=result, response=response
        )

    def _retry_after_execution_error(
        self, question: str, cypher: str, error: Exception
    ) -> CypherQuery | None:
        retry_fn = getattr(self.translator, "translate_with_execution_error", None)
        if not callable(retry_fn):
            return None
        self._logger.warning("query execution failed; attempting repair: %s", error)
        outcome = retry_fn(question, cypher, str(error), max_attempts=1)
        if outcome.cypher is None:
            self._logger.warning("repair translation failed: %s", outcome.error)
            return None
        self._logger.info("execution-guided repair produced replacement cypher")
        return CypherQuery(text=outcome.cypher)
